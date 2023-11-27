__author__ = "Johannes Köster, Andreas Wilm, Jake VanCampen"
__copyright__ = "Copyright 2023, Snakemake community"
__email__ = "johannes.koester@uni-due.de"
__license__ = "MIT"

from dataclasses import dataclass, field
import os
import datetime
import io
import re
import shlex
import sys
import uuid
from pprint import pformat
from urllib.parse import urlparse
from typing import AsyncGenerator, List, Optional

from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins.settings import (
    ExecutorSettingsBase,
    CommonSettings,
)
from snakemake_interface_executor_plugins.jobs import (
    JobExecutorInterface,
)
from snakemake_interface_common.exceptions import WorkflowError

from azure.core.pipeline import PipelineContext, PipelineRequest
from azure.core.pipeline.transport import HttpRequest
from azure.core.pipeline.policies import BearerTokenCredentialPolicy
from azure.identity import DefaultAzureCredential
from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
from azure.mgmt.batch import BatchManagementClient
import azure.batch._batch_service_client as bsc
import azure.batch.models as batchmodels
import azure.mgmt.batch.models as mgmtbatchmodels
import msrest.authentication as msa


# Optional:
# define additional settings for your executor
# They will occur in the Snakemake CLI as --<executor-name>-<param-name>
# Omit this class if you don't need any.
@dataclass
class ExecutorSettings(ExecutorSettingsBase):
    account_url: Optional[str] = field(
        default=None,
        metadata={
            "help": "Azure batch account url.",
            "required": True,
            "env_var": True,
        },
    )
    account_key: Optional[str] = field(
        default=None,
        metadata={
            "help": "Azure batch account key.",
            "required": True,
            "env_var": True,
        },
    )
    pool_subnet_id: Optional[str] = field(
        default=None,
        metadata={
            "help": "Azure batch pool subnet id.",
            "required": False,
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
    managed_identity_resource_id: Optional[str] = field(
        default=None,
        metadata={
            "help": "Azure managed identity resource id.",
            "required": False,
            "env_var": True,
        },
    )
    managed_identity_client_id: Optional[str] = field(
        default=None,
        metadata={
            "help": "Azure managed identity client id.",
            "required": False,
            "env_var": True,
        },
    )
    node_start_task_sasurl: Optional[str] = field(
        default=None,
        metadata={
            "help": "Azure batch node start task bash script sas url.",
            "required": False,
            "env_var": False,
        },
    )
    pool_image_publisher: str = field(
        default="microsoft-azure-batch",
        metadata={
            "help": "Azure batch pool image publisher.",
            "required": False,
            "env_var": False,
        },
    )
    pool_image_offer: str = field(
        default="ubuntu-server-container",
        metadata={
            "help": "Azure batch pool image offer.",
            "required": False,
            "env_var": False,
        },
    )
    pool_image_sku: str = field(
        default="20-04-lts",
        metadata={
            "help": "Azure batch pool image sku.",
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
    tasks_per_node: int = field(
        default=1,
        metadata={
            "help": "Azure batch tasks per node.",
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
            "help": "Azure batch node communication mode.",
            "required": False,
            "env_var": False,
        },
    )
    container_image: Optional[str] = field(
        default="snakemake/snakemake:latest",
        metadata={
            "help": "The snakemake base image used to run snakemake",
            "required": False,
            "env_var": True
        }
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
    container_registry_pass: Optional[str] = field(
        default=None,
        metadata={
            "help": "Azure container registry password.",
            "required": False,
            "env_var": True,
        },
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
    pass_envvar_declarations_to_cmd=False,
    auto_deploy_default_storage_provider=True,
)


# Required:
# Implementation of your executor
class Executor(RemoteExecutor):
    def __post_init__(self):
        AZURE_BATCH_RESOURCE_ENDPOINT = "https://batch.core.windows.net/"

        # setup batch configuration sets self.batch_config
        self.batch_config = AzBatchConfig(
            executor_settings=self.workflow.executor_settings
        )
        self.logger.debug(f"AzBatchConfig: {self.mask_batch_config_as_string()}")

        # handle case on OSX with /var/ symlinked to /private/var/ causing
        # issues with workdir not matching other workflow file dirs
        dirname = os.path.dirname(self.workflow.persistence.path)
        osxprefix = "/private"
        if osxprefix in dirname:
            dirname = dirname.removeprefix(osxprefix)

        self.workdir = dirname

        # Pool ids can only contain any combination of alphanumeric characters along
        # with dash and underscore.
        ts = datetime.datetime.now().strftime("%Y-%m%dT%H-%M-%S")
        self.pool_id = f"snakepool-{ts:s}"
        self.job_id = f"snakejob-{ts:s}"

        self.envvars = list(self.workflow.envvars) or []

        # enable autoscale flag
        self.az_batch_enable_autoscale = self.workflow.executor_settings.autoscale

        # authenticate batch client from SharedKeyCredentials
        if (
            self.batch_config.batch_account_key is not None
            and self.batch_config.managed_identity_client_id is None
        ):
            self.logger.debug("Using batch account key for authentication...")
            creds = SharedKeyCredentials(
                self.batch_config.batch_account_name,
                self.batch_config.batch_account_key,
            )
        # else authenticate with managed indentity client id
        elif self.batch_config.managed_identity_client_id is not None:
            self.logger.debug("Using managed identity batch authentication...")
            creds = DefaultAzureCredential(
                managed_identity_client_id=self.batch_config.managed_identity_client_id
            )
            creds = AzureIdentityCredentialAdapter(
                credential=creds, resource_id=AZURE_BATCH_RESOURCE_ENDPOINT
            )

        self.batch_client = BatchServiceClient(
            creds, batch_url=self.batch_config.batch_account_url
        )

        if self.batch_config.managed_identity_resource_id is not None:
            self.batch_mgmt_client = BatchManagementClient(
                credential=DefaultAzureCredential(
                    managed_identity_client_id=self.batch_config.managed_identity_client_id  # noqa
                ),
                subscription_id=self.batch_config.subscription_id,
            )

        self.create_batch_pool()
        self.create_batch_job()

    def shutdown(self):
        # perform additional steps on shutdown
        # if necessary (jobs were cancelled already)

        self.logger.debug("Deleting AzBatch job")
        self.batch_client.job.delete(self.job_id)

        self.logger.debug("Deleting AzBatch pool")
        self.batch_client.pool.delete(self.pool_id)

        super().shutdown()

    def run_job(self, job: JobExecutorInterface):
        # Implement here how to run a job.
        # You can access the job's resources, etc.
        # via the job object.
        # After submitting the job, you have to call
        # self.report_job_submission(job_info).
        # with job_info being of type
        # snakemake_interface_executor_plugins.executors.base.SubmittedJobInfo.
        import azure.batch._batch_service_client as batch

        envsettings = []
        for key in self.envvars:
            try:
                envsettings.append(
                    batchmodels.EnvironmentSetting(name=key, value=os.environ[key])
                )
            except KeyError:
                continue

        exec_job = self.format_job_exec(job)
        exec_job = f"/bin/bash -c '{shlex.quote(exec_job)}'"

        # A string that uniquely identifies the Task within the Job.
        task_uuid = str(uuid.uuid1())
        task_id = f"{job.rule.name}-{task_uuid}"

        # This is the admin user who runs the command inside the container.
        user = batchmodels.AutoUserSpecification(
            scope=batchmodels.AutoUserScope.pool,
            elevation_level=batchmodels.ElevationLevel.admin,
        )

        # This is the docker image we want to run
        task_container_settings = batchmodels.TaskContainerSettings(
            image_name=self.workflow.executor_settings.container_image, container_run_options="--rm"
        )

        # https://docs.microsoft.com/en-us/python/api/azure-batch/azure.batch.models.taskaddparameter?view=azure-python # noqa
        # all directories recursively below the AZ_BATCH_NODE_ROOT_DIR (the root of
        # Azure Batch directories on the node)
        # are mapped into the container, all Task environment variables are mapped into
        # the container, and the Task command line is executed in the container
        task = batch.models.TaskAddParameter(
            id=task_id,
            command_line=exec_job,
            container_settings=task_container_settings,
            user_identity=batchmodels.UserIdentity(auto_user=user),
            environment_settings=envsettings,
        )

        # register job as active, using your own namedtuple.
        self.batch_client.task.add(self.job_id, task)

        job_info = SubmittedJobInfo(job, external_jobid=task_id)
        self.logger.info(f"Added AzBatch task {task_id}")
        self.logger.debug(
            f"Task details: {pformat(self.mask_sas_urls(task.__dict__), indent=2)}"
        )
        self.report_job_submission(job_info)

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
                task = self.batch_client.task.get(self.job_id, batch_job.task_id)

            if task.state == batchmodels.TaskState.completed:
                stderr = self._get_task_output(self.job_id, batch_job.task_id, "stderr")
                stdout = self._get_task_output(self.job_id, batch_job.task_id, "stdout")

                if (
                    task.execution_info.result
                    == batchmodels.TaskExecutionResult.failure
                ):
                    self.report_job_error(batch_job, stderr=stderr, stdout=stdout)
                elif (
                    task.execution_info.result
                    == batchmodels.TaskExecutionResult.success
                ):
                    self.report_job_success(batch_job)
                else:
                    self.logger.error(
                        "Unknown Azure task execution result: {}".format(
                            task.execution_info.result
                        )
                    )
                    self.report_job_error(batch_job, stderr=stderr, stdout=stdout)

            # The operation is still running
            else:
                self.logger.debug(
                    f"task {batch_job.task_id}: creation_time={task.creation_time} "
                    f"state={task.state} node_info={task.node_info}\n"
                )
                # report as still running
                yield batch_job

                # fail if start task fails on a node or node state becomes unusable
                # and stream stderr stdout to stream
                node_list = self.batch_client.compute_node.list(self.pool_id)
                for n in node_list:
                    # error on unusable node (this occurs if your container image fails
                    # to pull)
                    if n.state == "unusable":
                        if n.errors is not None:
                            for e in n.errors:
                                self.logger.error(
                                    f"Azure task error: {e.message}, "
                                    f"{e.error_details[0].__dict__}"
                                )
                        self.logger.error("A node entered an unusable state, quitting.")
                        return

                    if n.start_task_info is not None and (
                        n.start_task_info.result
                        == batchmodels.TaskExecutionResult.failure
                    ):
                        try:
                            stderr_file = self.batch_client.file.get_from_compute_node(
                                self.pool_id, n.id, "/startup/stderr.txt"
                            )
                            stderr_stream = self._read_stream_as_string(
                                stderr_file, "utf-8"
                            )
                        except Exception:
                            stderr_stream = ""

                        try:
                            stdout_file = self.batch_client.file.get_from_compute_node(
                                self.pool_id, n.id, "/startup/stdout.txt"
                            )
                            stdout_stream = self._read_stream_as_string(
                                stdout_file, "utf-8"
                            )
                        except Exception:
                            stdout_stream = ""

                        self.logger.error(
                            "Azure start task execution failed on node: {}.\n"
                            "START_TASK_STDERR:{}\nSTART_TASK_STDOUT: {}".format(
                                n.start_task_info.failure_info.message,
                                stdout_stream,
                                stderr_stream,
                            )
                        )
                        return

    def cancel_jobs(self, active_jobs: List[SubmittedJobInfo]):
        # Cancel all active jobs.
        # This method is called when Snakemake is interrupted.
        # Since the batch client has its own list of jobs, we don't need
        # to access the given active_jobs here.
        for task in self.batch_client.task.list(self.job_id):
            # strictly not need as job deletion also deletes task
            self.batch_client.task.terminate(self.job_id, task.id)

    def create_batch_pool(self):
        """Creates a pool of compute nodes"""

        image_ref = bsc.models.ImageReference(
            publisher=self.batch_config.batch_pool_image_publisher,
            offer=self.batch_config.batch_pool_image_offer,
            sku=self.batch_config.batch_pool_image_sku,
            version="latest",
        )

        # optional subnet network configuration
        # requires AAD batch auth insead of batch key auth
        network_config = None
        if self.batch_config.batch_pool_subnet_id is not None:
            network_config = batchmodels.NetworkConfiguration(
                subnet_id=self.batch_config.batch_pool_subnet_id
            )

        # configure a container registry

        # Specify container configuration, fetching an image
        #  https://docs.microsoft.com/en-us/azure/batch/batch-docker-container-workloads#prefetch-images-for-container-configuration
        container_config = batchmodels.ContainerConfiguration(
            type="dockerCompatible", container_image_names=[self.workflow.executor_settings.container_image]
        )

        user = None
        passw = None
        identity_ref = None
        registry_conf = None

        if self.batch_config.container_registry_url is not None:
            if (
                self.batch_config.container_registry_user is not None
                and self.batch_config.container_registry_pass is not None
            ):
                user = self.batch_config.container_registry_user
                passw = self.batch_config.container_registry_pass
            elif self.batch_config.managed_identity_resource_id is not None:
                identity_ref = batchmodels.ComputeNodeIdentityReference(
                    resource_id=self.batch_config.managed_identity_resource_id
                )
            else:
                raise WorkflowError(
                    "No container registry authentication scheme set. Please set the "
                    "BATCH_CONTAINER_REGISTRY_USER and BATCH_CONTAINER_REGISTRY_PASS "
                    "or set MANAGED_IDENTITY_CLIENT_ID and "
                    "MANAGED_IDENTITY_RESOURCE_ID."
                )

            registry_conf = [
                batchmodels.ContainerRegistry(
                    registry_server=self.batch_config.container_registry_url,
                    identity_reference=identity_ref,
                    user_name=str(user),
                    password=str(passw),
                )
            ]

            # Specify container configuration, fetching an image
            #  https://docs.microsoft.com/en-us/azure/batch/batch-docker-container-workloads#prefetch-images-for-container-configuration
            container_config = batchmodels.ContainerConfiguration(
                type="dockerCompatible",
                container_image_names=[self.workflow.executor_settings.container_image],
                container_registries=registry_conf,
            )

        # default to no start task
        start_task = None

        # if configured us start task bash script from sas url
        if self.batch_config.batch_node_start_task_sasurl is not None:
            _SIMPLE_TASK_NAME = "start_task.sh"
            start_task_admin = batchmodels.UserIdentity(
                auto_user=batchmodels.AutoUserSpecification(
                    elevation_level=batchmodels.ElevationLevel.admin,
                    scope=batchmodels.AutoUserScope.pool,
                )
            )
            start_task = batchmodels.StartTask(
                command_line=f"bash {_SIMPLE_TASK_NAME}",
                resource_files=[
                    batchmodels.ResourceFile(
                        file_path=_SIMPLE_TASK_NAME,
                        http_url=self.batch_config.batch_node_start_task_sasurl,
                    )
                ],
                user_identity=start_task_admin,
            )

        # autoscale requires the initial dedicated node count to be zero
        if self.az_batch_enable_autoscale:
            self.batch_config.batch_pool_node_count = 0

        node_communication_strategy = None
        if self.batch_config.batch_node_communication_mode is not None:
            node_communication_strategy = batchmodels.NodeCommunicationMode.simplified

        new_pool = batchmodels.PoolAddParameter(
            id=self.pool_id,
            virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
                image_reference=image_ref,
                container_configuration=container_config,
                node_agent_sku_id=self.batch_config.batch_pool_vm_node_agent_sku_id,
            ),
            network_configuration=network_config,
            vm_size=self.batch_config.batch_pool_vm_size,
            target_dedicated_nodes=self.batch_config.batch_pool_node_count,
            target_node_communication_mode=node_communication_strategy,
            target_low_priority_nodes=0,
            start_task=start_task,
            task_slots_per_node=self.batch_config.batch_tasks_per_node,
            task_scheduling_policy=batchmodels.TaskSchedulingPolicy(
                node_fill_type=self.batch_config.batch_node_fill_type
            ),
        )

        # create pool if not exists
        try:
            self.logger.info(f"Creating pool: {self.pool_id}")
            self.batch_client.pool.add(new_pool)

            if self.az_batch_enable_autoscale:
                # define the autoscale formula
                formula = (
                    "$samples = $PendingTasks.GetSamplePercent(TimeInterval_Minute * 5);"  # noqa
                    "$tasks = $samples < 70 ? max(0,$PendingTasks.GetSample(1)) : max( $PendingTasks.GetSample(1), avg($PendingTasks.GetSample(TimeInterval_Minute * 5)));"  # noqa
                    "$targetVMs = $tasks > 0? $tasks:max(0, $TargetDedicatedNodes/2);"
                    "$TargetDedicatedNodes = max(0, min($targetVMs, 10));"
                    "$NodeDeallocationOption = taskcompletion;"
                )

                # Enable autoscale; specify the formula
                self.batch_client.pool.enable_auto_scale(
                    self.pool_id,
                    auto_scale_formula=formula,
                    # the minimum allowed autoscale interval is 5 minutes
                    auto_scale_evaluation_interval=datetime.timedelta(minutes=5),
                    pool_enable_auto_scale_options=None,
                    custom_headers=None,
                    raw=False,
                )

            # update pool with managed identity, enables batch nodes to act as managed
            # identity
            if self.batch_config.managed_identity_resource_id is not None:
                mid = mgmtbatchmodels.BatchPoolIdentity(
                    type=mgmtbatchmodels.PoolIdentityType.user_assigned,
                    user_assigned_identities={
                        self.batch_config.managed_identity_resource_id: mgmtbatchmodels.UserAssignedIdentities()  # noqa
                    },
                )
                params = mgmtbatchmodels.Pool(identity=mid)
                self.batch_mgmt_client.pool.update(
                    resource_group_name=self.batch_config.resource_group,
                    account_name=self.batch_config.batch_account_name,
                    pool_name=self.pool_id,
                    parameters=params,
                )

        except batchmodels.BatchErrorException as err:
            if err.error.code != "PoolExists":
                raise WorkflowError(
                    f"Error: Failed to create pool: {err.error.message}"
                )
            else:
                self.logger.info(f"Pool {self.pool_id} exists.")

    def create_batch_job(self):
        """Creates a job with the specified ID, associated with the specified pool"""

        self.logger.info(f"Creating batch job {self.job_id}")

        self.batch_client.job.add(
            bsc.models.JobAddParameter(
                id=self.job_id,
                constraints=bsc.models.JobConstraints(max_task_retry_count=0),
                pool_info=bsc.models.PoolInformation(pool_id=self.pool_id),
            )
        )

    # mask_dict_vals masks sensitive keys from a dictionary of values for
    # logging used to mask dicts with sensitive information from logging
    @staticmethod
    def mask_dict_vals(mdict: dict, keys: list):
        ret_dict = mdict.copy()
        for k in keys:
            if k in ret_dict.keys() and ret_dict[k] is not None:
                ret_dict[k] = 10 * "*"
        return ret_dict

    # mask blob url is used to mask url values that may contain SAS
    # token information from being printed to the logs
    def mask_sas_urls(self, attrs: dict):
        attrs_new = attrs.copy()
        sas_pattern = r"\?[^=]+=([^?'\"]+)"
        mask = 10 * "*"

        for k, value in attrs.items():
            if value is not None and re.search(sas_pattern, str(value)):
                attrs_new[k] = re.sub(sas_pattern, mask, value)

        return attrs_new

    def mask_batch_config_as_string(self) -> str:
        masked_keys = self.mask_dict_vals(
            self.batch_config.__dict__,
            [
                "batch_account_key",
                "managed_identity_client_id",
            ],
        )
        masked_urls = self.mask_sas_urls(masked_keys)
        return pformat(masked_urls, indent=2)

    # from https://github.com/Azure-Samples/batch-python-quickstart/blob/master/src/python_quickstart_client.py # noqa
    @staticmethod
    def _read_stream_as_string(stream, encoding):
        """Read stream as string
        :param stream: input stream generator
        :param str encoding: The encoding of the file. The default is utf-8.
        :return: The file content.
        :rtype: str
        """
        output = io.BytesIO()
        try:
            for data in stream:
                output.write(data)
            if encoding is None:
                encoding = "utf-8"
            return output.getvalue().decode(encoding)
        finally:
            output.close()

    # adopted from
    # https://github.com/Azure-Samples/batch-python-quickstart/blob/master/src/python_quickstart_client.py # noqa
    def _get_task_output(self, job_id, task_id, stdout_or_stderr, encoding=None):
        assert stdout_or_stderr in ["stdout", "stderr"]
        fname = stdout_or_stderr + ".txt"
        try:
            stream = self.batch_client.file.get_from_task(job_id, task_id, fname)
            content = self._read_stream_as_string(stream, encoding)
        except Exception:
            content = ""

        return content


class AzBatchConfig:
    def __init__(self, executor_settings: ExecutorSettings):
        # configure defaults
        self.batch_account_url = executor_settings.account_url

        # parse batch account name
        result = urlparse(self.batch_account_url)
        self.batch_account_name = str.split(result.hostname, ".")[0]

        self.batch_account_key = executor_settings.account_key

        # optional subnet config
        self.batch_pool_subnet_id = executor_settings.pool_subnet_id

        # managed identity resource id configuration
        self.managed_identity_resource_id = (
            executor_settings.managed_identity_resource_id
        )

        # parse subscription and resource id
        if self.managed_identity_resource_id is not None:
            self.subscription_id = self.managed_identity_resource_id.split("/")[2]
            self.resource_group = self.managed_identity_resource_id.split("/")[4]

        self.managed_identity_client_id = executor_settings.managed_identity_client_id

        if self.batch_pool_subnet_id is not None:
            if (
                self.managed_identity_client_id is None
                or self.managed_identity_resource_id is None
            ):
                sys.exit(
                    "Error: managed_identity_resource_id, "
                    "managed_identity_client_id must be set when deploying batch "
                    "nodes into a private subnet!"
                )

            # parse account details necessary for batch client authentication steps
            if self.batch_pool_subnet_id.split("/")[2] != self.subscription_id:
                raise WorkflowError(
                    "Error: managed identity must be in the same subscription as the "
                    "batch pool subnet."
                )

            if self.batch_pool_subnet_id.split("/")[4] != self.resource_group:
                raise WorkflowError(
                    "Error: managed identity must be in the same resource group "
                    "as the batch pool subnet."
                )

        # sas url to a batch node start task bash script
        self.batch_node_start_task_sasurl = executor_settings.node_start_task_sasurl

        # options configured with env vars or default
        self.batch_pool_image_publisher = executor_settings.pool_image_publisher
        self.batch_pool_image_offer = executor_settings.pool_image_offer
        self.batch_pool_image_sku = executor_settings.pool_image_sku
        self.batch_pool_vm_node_agent_sku_id = (
            executor_settings.pool_vm_node_agent_sku_id
        )
        self.batch_pool_vm_size = executor_settings.pool_vm_size

        # dedicated pool node count
        self.batch_pool_node_count = executor_settings.pool_node_count

        # default tasks per node
        # see https://learn.microsoft.com/en-us/azure/batch/batch-parallel-node-tasks
        self.batch_tasks_per_node = executor_settings.tasks_per_node

        # possible values "spread" or "pack"
        # see https://learn.microsoft.com/en-us/azure/batch/batch-parallel-node-tasks
        self.batch_node_fill_type = executor_settings.node_fill_type

        # enables simplified batch node communication if set
        # see: https://learn.microsoft.com/en-us/azure/batch
        # /simplified-compute-node-communication
        self.batch_node_communication_mode = executor_settings.node_communication_mode

        self.container_registry_url = executor_settings.container_registry_url

        self.container_registry_user = executor_settings.container_registry_user

        self.container_registry_pass = executor_settings.container_registry_pass

    @staticmethod
    def set_or_default(evar: str, default: Optional[str]):
        gotvar = os.getenv(evar)
        if gotvar is not None:
            return gotvar
        else:
            return default


# The usage of this credential helper is required to authenitcate batch with managed
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

        Default resource is ARM (syntax of endpoint v2)
        :param credential: Any azure-identity credential (DefaultAzureCredential by
                           default)
        :param str resource_id: The scope to use to get the token (default ARM)
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
        Using the policy gives us for free the caching system of azure-core.
        We could make this code simpler by using private method, but by definition
        I can't assure they will be there forever, so mocking a fake call to the policy
        to extract the token, using 100% public API."""
        request = self._make_request()
        self._policy.on_request(request)
        # Read Authorization, and get the second part after Bearer
        token = request.http_request.headers["Authorization"].split(" ", 1)[1]
        self.token = {"access_token": token}

    def signed_session(self, session=None):
        self.set_token()
        return super(AzureIdentityCredentialAdapter, self).signed_session(session)
