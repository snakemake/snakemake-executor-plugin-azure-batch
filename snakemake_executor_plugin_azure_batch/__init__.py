__author__ = "Jake VanCampen, Johannes Köster, Andreas Wilm"
__copyright__ = "Copyright 2023, Snakemake community"
__email__ = "jake.vancampen7@gmail.com"
__license__ = "MIT"


import datetime
import shlex
from dataclasses import dataclass, field
from pprint import pformat
from typing import AsyncGenerator, List, Optional
from urllib.parse import urlparse

import azure.batch._batch_service_client as bsc
import azure.batch.models as bm
from azure.batch import BatchServiceClient
from azure.core.exceptions import HttpResponseError
from azure.identity import DefaultAzureCredential
from azure.mgmt.batch import BatchManagementClient
from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins.jobs import JobExecutorInterface
from snakemake_interface_executor_plugins.settings import (
    CommonSettings,
    ExecutorSettingsBase,
)

from snakemake_executor_plugin_azure_batch import build
from snakemake_executor_plugin_azure_batch.constant import AZURE_BATCH_RESOURCE_ENDPOINT
from snakemake_executor_plugin_azure_batch.util import (
    AzureIdentityCredentialAdapter,
    read_stream_as_string,
    unpack_compute_node_errors,
    unpack_task_failure_information,
)

# Required:
# Specify common settings shared by various executors.
common_settings = CommonSettings(
    # define whether your executor plugin executes locally
    # or remotely. In virtually all cases, it will be remote execution
    # (cluster, cloud, etc.). Only Snakemake's standard execution
    # plugins (snakemake-executor-plugin-dryrun, snakemake-executor-plugin-local)
    # are expected to specify False here.
    non_local_exec=True,
    # Define whether your executor plugin implies that there is no shared
    # filesystem (True) or not (False).
    # This is e.g. the case for cloud execution.
    implies_no_shared_fs=True,
    job_deploy_sources=True,
    pass_default_storage_provider_args=True,
    pass_default_resources_args=True,
    pass_envvar_declarations_to_cmd=True,
    auto_deploy_default_storage_provider=True,
)


# Optional:
# define additional settings for your executor
# They will occur in the Snakemake CLI as --<executor-name>-<param-name>
# Omit this class if you don't need any.
@dataclass
class ExecutorSettings(ExecutorSettingsBase):
    account_url: Optional[str] = field(
        default=None,
        metadata={
            "help": "Batch account url: https://<account>.<region>.batch.azure.com",
            "required": True,
            "env_var": True,
        },
    )
    autoscale: bool = field(
        default=False,
        metadata={
            "help": "Enable autoscaling of the azure batch pool nodes, this option "
            "will set the initial dedicated node count to zero, and requires five "
            "minutes to resize the cluster, so is only recommended for longer "
            "running workflows.",
            "required": False,
            "env_var": False,
        },
    )
    container_registry_pass: Optional[str] = field(
        default=None,
        metadata={
            "help": "Azure container registry password.",
            "required": False,
            "env_var": True,
        },
    )
    container_registry_url: Optional[str] = field(
        default=None,
        metadata={
            "help": "Azure container registry url.",
            "required": False,
            "env_var": True,
        },
    )
    container_registry_user: Optional[str] = field(
        default=None,
        metadata={
            "help": "Azure container registry user.",
            "required": False,
            "env_var": True,
        },
    )
    keep_pool: bool = field(
        default=False,
        metadata={
            "help": "Keep the Azure Batch resources after the workflow finished.",
            "required": False,
            "env_var": False,
        },
    )
    managed_identity_resource_id: Optional[str] = field(
        default=None,
        metadata={
            "help": "Azure Managed Identity resource id. Managed identity is used for"
            "authentication of the Azure Batch nodes to other Azure resources. Required"
            "if using the Snakemake Azure Storage plugin or if you need access to"
            " Azure Container registry from the nodes.",
            "required": False,
            "env_var": True,
        },
    )
    managed_identity_client_id: Optional[str] = field(
        default=None,
        repr=False,
        metadata={
            "help": "Azure Managed Identity client id.",
            "required": False,
            "env_var": True,
        },
    )
    node_start_task_url: Optional[str] = field(
        default=None,
        metadata={
            "help": "Azure Batch node start task bash script url."
            "This can be any url that hosts your start task bash script. Azure blob SAS"
            "urls work nicely here",
            "required": False,
            "env_var": False,
        },
    )
    node_fill_type: str = field(
        default="spread",
        metadata={
            "help": "Azure batch node fill type.",
            "required": False,
            "env_var": False,
        },
    )
    node_communication_mode: Optional[str] = field(
        default=None,
        metadata={
            "help": "Azure Batch node communication mode.",
            "required": False,
            "env_var": False,
        },
    )
    pool_subnet_id: Optional[str] = field(
        default=None,
        metadata={
            "help": "Azure Batch pool subnet id.",
            "required": False,
            "env_var": True,
        },
    )
    pool_image_publisher: str = field(
        default="microsoft-azure-batch",
        metadata={
            "help": "Batch pool image publisher.",
            "required": False,
            "env_var": False,
        },
    )
    pool_image_offer: str = field(
        default="ubuntu-server-container",
        metadata={
            "help": "Batch pool image offer.",
            "required": False,
            "env_var": False,
        },
    )
    pool_image_sku: str = field(
        default="20-04-lts",
        metadata={
            "help": "Batch pool image sku.",
            "required": False,
            "env_var": False,
        },
    )
    pool_vm_node_agent_sku_id: str = field(
        default="batch.node.ubuntu 20.04",
        metadata={
            "help": "Azure batch pool vm node agent sku id.",
            "required": False,
            "env_var": False,
        },
    )
    pool_vm_size: str = field(
        default="Standard_D2_v3",
        metadata={
            "help": "Azure batch pool vm size.",
            "required": False,
            "env_var": False,
        },
    )
    pool_node_count: int = field(
        default=1,
        metadata={
            "help": "Azure batch pool node count.",
            "required": False,
            "env_var": False,
        },
    )
    resource_group_name: Optional[str] = field(
        default=None,
        metadata={
            "help": "The name of the Azure Resource Group "
            "containing the Azure Batch Account",
            "required": True,
            "env_var": True,
        },
    )
    subscription_id: Optional[str] = field(
        default=None,
        metadata={
            "help": "The Azure Subscription ID of the Azure Batch Account",
            "required": True,
            "env_var": True,
        },
    )
    tasks_per_node: int = field(
        default=1,
        metadata={
            "help": "Batch tasks per node. If node count is greater than 1, this option"
            "helps optimize the number of tasks each node can handle simultaneously.",
            "required": False,
            "env_var": False,
        },
    )

    @property
    def batch_account_name(self):
        """return the batch account name of the batch account url"""
        return urlparse(self.account_url).netloc.split(".")[0]


class Executor(RemoteExecutor):
    def __post_init__(self):
        # the snakemake/snakemake:latest container image to run the workflow remote
        self.container_image = self.workflow.remote_execution_settings.container_image
        self.settings: ExecutorSettings = self.workflow.executor_settings
        self.logger.debug(
            f"ExecutorSettings: {pformat(self.workflow.executor_settings, indent=2)}"
        )

        # Pool ids can only contain alphanumeric characters, dashes and underscore.
        ts = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        self.pool_id = f"snakepool-{ts:s}"
        self.job_id = f"snakejob-{ts:s}"

        # requires managed identity resource id to be set for the azure storage plugin
        if self.workflow.storage_settings.default_storage_provider == "azure":
            if self.settings.managed_identity_resource_id is None:
                raise WorkflowError(
                    "Azure Storage plugin requires a managed identity "
                    "resource and client ids to be set."
                )

        self.init_batch_client()
        self.create_batch_pool()
        self.create_batch_job()

    def init_batch_client(self):
        """
        Initialize the BatchServiceClient and BatchManagementClient using
        DefaultAzureCredential.

        Sets:
            - self.batch_client
            - self.batch_mgmt_client
        """
        try:

            # initialize BatchServiceClient
            default_credential = DefaultAzureCredential(
                exclude_managed_identity_credential=True
            )
            adapted_credential = AzureIdentityCredentialAdapter(
                credential=default_credential, resource_id=AZURE_BATCH_RESOURCE_ENDPOINT
            )
            self.batch_client = BatchServiceClient(
                adapted_credential, self.settings.account_url
            )

            # initialize BatchManagementClient
            self.batch_mgmt_client = BatchManagementClient(
                credential=default_credential,
                subscription_id=self.settings.subscription_id,
            )

        except Exception as e:
            raise WorkflowError("Failed to initialize batch clients", e)

    def cleanup_resources(self):
        """Cleanup Azure Batch resources.

        This method is responsible for cleaning up Azure Batch resources, including
        deleting the Batch job and pool. If the `keep_pool` setting is enabled, the
        pool will not be deleted.

        Raises:
            WorkflowError: If there is an error deleting the Batch job or pool.
        """
        if not self.settings.keep_pool:
            try:
                self.logger.debug("Deleting AzBatch job")
                self.batch_client.job.delete(self.job_id)
            except bm.BatchErrorException as be:
                if be.error.code == "JobNotFound":
                    pass

            try:
                self.logger.debug("Deleting AzBatch pool")
                self.batch_client.pool.delete(self.pool_id)
            except bm.BatchErrorException as be:
                if be.error.code == "PoolBeingDeleted":
                    pass

    def shutdown(self):
        """Shutdown the executor, cleaning up resources if necessary.

        This method is responsible for shutting down the executor and cleaning up any
        resources that were created during the execution. If the `keep_pool` setting is
        enabled, the pool will not be deleted. This method should be called before
        exiting the program or when the executor is no longer needed.
        """
        self.cleanup_resources()
        super().shutdown()

    def run_job(self, job: JobExecutorInterface):
        # Implement here how to run a job.
        # You can access the job's resources, etc.
        # via the job object.
        # After submitting the job, you have to call
        # self.report_job_submission(job_info).
        # with job_info being of type
        # snakemake_interface_executor_plugins.executors.base.SubmittedJobInfo.
        remote_command = f"/bin/bash -c {shlex.quote(self.format_job_exec(job))}"
        self.logger.debug(f"Remote command: {remote_command}")

        task: bm.TaskAddParameter = build.batch_task(
            job, self.envvars(), remote_command
        )

        job_info = SubmittedJobInfo(job, external_jobid=task.id)

        # register job as active, using your own namedtuple.
        try:
            self.batch_client.task.add(self.job_id, task)
        except Exception as e:
            self.report_job_error(job_info, msg=f"Unable to add batch task: {e}")

        self.report_job_submission(job_info)

    def _report_pool_errors(self, job: SubmittedJobInfo):
        """Report batch pool errors.

        This method is responsible for reporting any resize errors that are detected
        from the Azure Batch pool.
        """
        errors = []
        pool = self.batch_client.pool.get(self.pool_id)
        if pool.resize_errors:
            for e in pool.resize_errors:
                err_dict = {"code": e.code, "message": e.message}
                errors.append(err_dict)
            self.report_job_error(job, msg=f"Batch pool error: {e}. ")

    def _report_task_status(self, job: SubmittedJobInfo):
        """Report batch task status.

        Returns:
            bool: True if the task is still running, False otherwise.
        """
        try:
            task: bm.CloudTask = self.batch_client.task.get(
                job_id=self.job_id, task_id=job.external_jobid
            )
        except Exception as e:
            self.logger.warning(f"Unable to get Azure Batch Task status: {e}")
            return True

        self.logger.debug(
            {
                "task": task.id,
                "state": str(task.state),
                "creation_time": str(task.creation_time),
            }
        )

        if task.state == bm.TaskState.completed:
            stderr = self._get_task_output(self.job_id, job.external_jobid, "stderr")
            stdout = self._get_task_output(self.job_id, job.external_jobid, "stdout")

            ei: bm.TaskExecutionInformation = task.execution_info
            if ei is not None:
                if ei.result == bm.TaskExecutionResult.failure:
                    formatted_failure_info = pformat(
                        unpack_task_failure_information(ei.failure_info), indent=2
                    )
                    msg = f"Batch Task Failure: {formatted_failure_info}\n"
                    self.report_job_error(job, msg=msg, stderr=stderr, stdout=stdout)
                elif ei.result == bm.TaskExecutionResult.success:
                    self.report_job_success(job)
            return False
        else:
            return True

    def _report_node_errors(self):
        """report node errors

        Fails if start task fails on a node, or node state becomes unusable (this can
        happen if the container configuration is incorrect).

        Streams stderr and stdout to on error.
        """

        node_list = self.batch_client.compute_node.list(self.pool_id)
        for n in node_list:
            if n.state == bm.ComputeNodeState.unusable:
                if n.errors:
                    errors = unpack_compute_node_errors(n)
                self.logger.error(f"An Azure Batch node became unusable: {errors}")

            if n.start_task_info is not None and (
                n.start_task_info.result == bm.TaskExecutionResult.failure
            ):
                try:
                    stderr_file = self.batch_client.file.get_from_compute_node(
                        self.pool_id, n.id, "/startup/stderr.txt"
                    )
                    stderr_stream = read_stream_as_string(stderr_file, "utf-8")
                except Exception:
                    stderr_stream = ""

                try:
                    stdout_file = self.batch_client.file.get_from_compute_node(
                        self.pool_id, n.id, "/startup/stdout.txt"
                    )
                    stdout_stream = read_stream_as_string(stdout_file, "utf-8")
                except Exception:
                    stdout_stream = ""

                msg = (
                    "Start task execution failed: "
                    f"{n.start_task_info.failure_info.message}.\n"
                    f"stderr:\n{stderr_stream}\n"
                    f"stdout:\n{stdout_stream}"
                )
                self.logger.error(msg)

    async def check_active_jobs(
        self, active_jobs: List[SubmittedJobInfo]
    ) -> AsyncGenerator[SubmittedJobInfo, None]:
        # Check the status of active jobs.

        # You have to iterate over the given list active_jobs.
        # For jobs that have finished successfully, you have to call
        # self.report_job_success(job).
        # For jobs that have errored, you have to call
        # self.report_job_error(job).
        # Jobs that are still running have to be yielded.
        #
        # For queries to the remote middleware, please use
        # self.status_rate_limiter like this:
        #
        # async with self.status_rate_limiter:
        #    # query remote middleware here
        self.logger.debug(f"Monitoring {len(active_jobs)} active AzBatch tasks")

        for batch_job in active_jobs:
            async with self.status_rate_limiter:
                self._report_pool_errors(batch_job)

            async with self.status_rate_limiter:
                self._report_node_errors()
                still_running = self._report_task_status(batch_job)
                if still_running:
                    yield batch_job

    def cancel_jobs(self, active_jobs: List[SubmittedJobInfo]):
        # Cancel all active jobs.
        # This method is called when Snakemake is interrupted.
        # Since the batch client has its own list of jobs, we don't need
        # to access the given active_jobs here.
        for task in self.batch_client.task.list(self.job_id):
            # strictly not need as job deletion also deletes task
            self.batch_client.task.terminate(self.job_id, task.id)

    def create_batch_pool(self):
        """Creates a pool of compute nodes.

        This method is responsible for creating a pool of compute nodes in Azure Batch.

        Returns:
            None

        Raises:
            WorkflowError: If there is an error creating the pool.
        """

        pool_params = build.batch_pool_params(
            pool_id=self.pool_id,
            settings=self.settings,
            container_image=self.container_image,
        )
        try:
            self.logger.info(f"Creating Batch Pool: {self.pool_id}")
            # we use the azure.mgmt.batch client to create the pool here because if you
            # configure a managed identity for the batch nodes, the azure.batch client
            # does not correctly apply it to the pool
            self.batch_mgmt_client.pool.create(
                resource_group_name=self.settings.resource_group_name,
                account_name=self.settings.batch_account_name,
                pool_name=self.pool_id,
                parameters=pool_params,
            )

        except HttpResponseError as err:
            if err.error.code != "PoolExists":
                raise WorkflowError(
                    f"Error: Failed to create pool: {err.error.message}"
                )
            else:
                self.logger.info(f"Pool {self.pool_id} exists.")

    def create_batch_job(self):
        """Creates a job with the specified ID, associated with the specified pool.

        Args:
            job_id (str): The ID of the job.
            pool_id (str): The ID of the pool associated with the job.

        Returns:
            None

        Raises:
            WorkflowError: If there is an error creating the job.
        """

        self.logger.info(f"Creating Batch Job: {self.job_id}")

        try:
            self.batch_client.job.add(
                bsc.models.JobAddParameter(
                    id=self.job_id,
                    constraints=bsc.models.JobConstraints(max_task_retry_count=0),
                    pool_info=bsc.models.PoolInformation(pool_id=self.pool_id),
                )
            )
        except bm.BatchErrorException as e:
            raise WorkflowError("Error adding batch job", e)

    # adopted from
    # https://github.com/Azure-Samples/batch-python-quickstart/blob/master/src/python_quickstart_client.py # noqa
    def _get_task_output(self, job_id, task_id, stdout_or_stderr, encoding=None):
        """
        Retrieves the content of the specified task's stdout or stderr file.

        Args:
            job_id (str): The ID of the job that contains the task.
            task_id (str): The ID of the task.
            stdout_or_stderr (str): Specifies whether to retrieve the stdout or stderr
                file content. Must be either "stdout" or "stderr".
            encoding (str, optional): The encoding to use when reading the file content.
            Defaults to None.

        Returns:
            str: The content of the specified stdout or stderr file, or an empty string
            if the file does not exist.

        Raises:
            Exception: If an error occurs while retrieving the file content.
        """
        assert stdout_or_stderr in ["stdout", "stderr"]
        fname = stdout_or_stderr + ".txt"
        try:
            stream = self.batch_client.file.get_from_task(job_id, task_id, fname)
            content = read_stream_as_string(stream, encoding)
        except Exception:
            content = ""

        return content
