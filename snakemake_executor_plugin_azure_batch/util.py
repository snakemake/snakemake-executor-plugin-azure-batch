import io
from typing import List

import msrest.authentication as msa
from azure.batch.models import ComputeNodeError, NameValuePair, TaskFailureInformation
from azure.core.pipeline import PipelineContext, PipelineRequest
from azure.core.pipeline.policies import BearerTokenCredentialPolicy
from azure.core.pipeline.transport import HttpRequest
from azure.identity import DefaultAzureCredential


# The usage of this credential helper is required to authenticate batch with managed
# identity credentials
# because not all Azure SDKs support the azure.identity credentials yet, and batch is
# one of them.
# ref1: https://gist.github.com/lmazuel/cc683d82ea1d7b40208de7c9fc8de59d
# ref2: https://gist.github.com/lmazuel/cc683d82ea1d7b40208de7c9fc8de59d
class AzureIdentityCredentialAdapter(msa.BasicTokenAuthentication):
    def __init__(
        self,
        credential=None,
        resource_id="https://management.azure.com/.default",
        **kwargs,
    ):
        """Adapt any azure-identity credential to work with SDK that needs
        azure.common.credentials or msrestazure.

        Args:
            credential: Any azure-identity credential (DefaultAzureCredential by
                   default)
            resource_id (str): The scope to use to get the token (default ARM)
        """
        super(AzureIdentityCredentialAdapter, self).__init__(None)
        if credential is None:
            credential = DefaultAzureCredential()
        self._policy = BearerTokenCredentialPolicy(credential, resource_id, **kwargs)

    def _make_request(self):
        return PipelineRequest(
            HttpRequest("AzureIdentityCredentialAdapter", "https://fakeurl"),
            PipelineContext(None),
        )

    def set_token(self):
        """Ask the azure-core BearerTokenCredentialPolicy policy to get a token.

        Using the policy gives us the benefit of the caching system provided by
        azure-core. Although we could simplify this code by using private methods,
        it's not guaranteed that they will be available in the future. Therefore,
        we mock a fake call to the policy using 100% public API to extract the token.
        """
        request = self._make_request()
        self._policy.on_request(request)
        # Read Authorization, and get the second part after Bearer
        token = request.http_request.headers["Authorization"].split(" ", 1)[1]
        self.token = {"access_token": token}

    def signed_session(self, session=None):
        self.set_token()
        return super(AzureIdentityCredentialAdapter, self).signed_session(session)


def _error_item(code: str, message: str) -> dict:
    return {
        "code": code,
        "message": message,
        "error_details": [],
    }


def unpack_task_failure_information(failure_info: TaskFailureInformation) -> dict:
    """
    Unpacks task failure information into an object.

    Args:
        failure_info (TaskFailureInformation): The task failure information.

    Returns:
        dict: The unpacked error item with the following structure:
            {
                'code': str,
                'message': str,
                'error_details': [
                    {
                        'detail': str
                    }
                ]
            }
    """
    error_item = _error_item(failure_info.code, failure_info.message)
    for detail in failure_info.details:
        if isinstance(detail, NameValuePair):
            error_item["error_details"].append({detail.name: detail.value})
    return error_item


def unpack_compute_node_errors(node_errors: List[ComputeNodeError]) -> list:
    """
    Unpack a list of compute node errors into a list of items.

    Args:
        node_errors (List[ComputeNodeError]): The list of compute node errors.

    Returns:
        List[dict]: The list of unpacked error items, each containing 'code',
        'message', and 'error_details'.

    """
    errors = []
    for node_error in node_errors:
        error_item = _error_item(node_error.code, node_error.message)
        if node_error.error_details:
            for detail in node_error.error_details:
                if isinstance(detail, NameValuePair):
                    error_item["error_details"].append({detail.name: detail.value})
        errors.append(error_item)
    return errors


def read_stream_as_string(stream, encoding="utf-8"):
    """Reads a stream as a string.

    Args:
        stream: The input stream generator.
        encoding: The encoding of the file. Default is utf-8.

    Returns:
        The file content as a string.

    Raises:
        None.
    """
    output = io.BytesIO()
    try:
        for data in stream:
            output.write(data)
        return output.getvalue().decode(encoding)
    finally:
        output.close()
