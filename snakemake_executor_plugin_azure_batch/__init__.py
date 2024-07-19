__author__ = "Jake VanCampen, Johannes Köster, Andreas Wilm"
__copyright__ = "Copyright 2023, Snakemake community"
__email__ = "jake.vancampen7@gmail.com"
__license__ = "MIT"


import datetime
import io
import os
import shlex
import uuid
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
from azure.mgmt.batch.models import (
    AutoScaleSettings,
    AutoUserScope,
    AutoUserSpecification,
    BatchPoolIdentity,
    ComputeNodeIdentityReference,
    ContainerConfiguration,
    ContainerRegistry,
    ContainerType,
    DeploymentConfiguration,
    ElevationLevel,
    FixedScaleSettings,
    ImageReference,
    NetworkConfiguration,
    NodeCommunicationMode,
    Pool,
    PoolIdentityType,
    ResourceFile,
    ScaleSettings,
    StartTask,
    TaskSchedulingPolicy,
    UserAssignedIdentities,
    UserIdentity,
    VirtualMachineConfiguration,
)
from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins.jobs import JobExecutorInterface
from snakemake_interface_executor_plugins.settings import (
    CommonSettings,
    ExecutorSettingsBase,
)

from snakemake_executor_plugin_azure_batch.constant import (
    AZURE_BATCH_RESOURCE_ENDPOINT,
    DEFAULT_AUTO_SCALE_FORMULA,
)
from snakemake_executor_plugin_azure_batch.util import (
    AzureIdentityCredentialAdapter,
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
        # the snakemake/snakemake:latest container image
        self.container_image = self.workflow.remote_execution_settings.container_image
        self.settings: ExecutorSettings = self.workflow.executor_settings
        self.logger.debug(
            f"ExecutorSettings: {pformat(self.workflow.executor_settings, indent=2)}"
        )

        # handle case on OSX with /var/ symlinked to /private/var/ causing
        # issues with workdir not matching other workflow file dirs
        dirname = os.path.dirname(self.workflow.persistence.path)
        osxprefix = "/private"
        if osxprefix in dirname:
            dirname = dirname.removeprefix(osxprefix)

        self.workdir = dirname

        # Pool ids can only contain any combination of alphanumeric characters along
        # with dash and underscore.
        ts = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        self.pool_id = f"snakepool-{ts:s}"
        self.job_id = f"snakejob-{ts:s}"

        self.envvars = self.workflow.spawned_job_args_factory.envvars()
        self.init_batch_client()
        self.create_batch_pool()
        self.create_batch_job()

    # override real.py until bugfix is released
    def get_envvar_declarations(self):
        if self.common_settings.pass_envvar_declarations_to_cmd:
            defs = " ".join(
                f"{var}={repr(value)}" for var, value in self.envvars.items()
            )
            if defs:
                return f"export {defs} &&"
            else:
                return ""
        else:
            return ""

    def init_batch_client(self):
        """
        Initialize the batch service client from the given credentials

        Sets:
            self.batch_client
            self.batch_mgmt_client
        """
        try:
            # alias these variables here to save space
            batch_url = self.settings.account_url

            # else authenticate with managed identity client id
            default_credential = DefaultAzureCredential(
                exclude_managed_identity_credential=True
            )
            adapted_credential = AzureIdentityCredentialAdapter(
                credential=default_credential, resource_id=AZURE_BATCH_RESOURCE_ENDPOINT
            )

            # initialize batch client with creds
            self.batch_client = BatchServiceClient(adapted_credential, batch_url)

            # initialize BatchManagementClient
            self.batch_mgmt_client = BatchManagementClient(
                credential=default_credential,
                subscription_id=self.settings.subscription_id,
            )

        except Exception as e:
            raise WorkflowError("Failed to initialize batch client", e)

    def shutdown(self):
        # perform additional steps on shutdown
        # if necessary (jobs were cancelled already)
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

        super().shutdown()

    def run_job(self, job: JobExecutorInterface):
        # Implement here how to run a job.
        # You can access the job's resources, etc.
        # via the job object.
        # After submitting the job, you have to call
        # self.report_job_submission(job_info).
        # with job_info being of type
        # snakemake_interface_executor_plugins.executors.base.SubmittedJobInfo.
        envsettings = []
        for key, value in self.envvars.items():
            try:
                envsettings.append(bm.EnvironmentSetting(name=key, value=value))
            except KeyError:
                continue

        exec_job = self.format_job_exec(job)
        remote_command = f"/bin/bash -c {shlex.quote(exec_job)}"
        self.logger.debug(f"Remote command: {remote_command}")

        # A string that uniquely identifies the Task within the Job.
        task_uuid = str(uuid.uuid4())
        task_id = f"{job.name}-{task_uuid}"

        # This is the admin user who runs the command inside the container.
        user = bm.AutoUserSpecification(
            scope=bm.AutoUserScope.pool,
            elevation_level=bm.ElevationLevel.admin,
        )

        # This is the docker image we want to run
        task_container_settings = bm.TaskContainerSettings(
            image_name=self.container_image,
            container_run_options="--rm",
        )

        # https://docs.microsoft.com/en-us/python/api/azure-batch/azure.batch.models.taskaddparameter?view=azure-python # noqa
        # all directories recursively below the AZ_BATCH_NODE_ROOT_DIR (the root of
        # Azure Batch directories on the node)
        # are mapped into the container, all Task environment variables are mapped into
        # the container, and the Task command line is executed in the container
        task = bm.TaskAddParameter(
            id=task_id,
            command_line=remote_command,
            container_settings=task_container_settings,
            user_identity=bm.UserIdentity(auto_user=user),
            environment_settings=envsettings,
        )

        job_info = SubmittedJobInfo(job, external_jobid=task_id)

        # register job as active, using your own namedtuple.
        try:
            self.batch_client.task.add(self.job_id, task)
        except Exception as e:
            self.report_job_error(job_info, msg=f"Unable to add batch task: {e}")

        self.report_job_submission(job_info)

    def _report_pool_errors(self, job: SubmittedJobInfo):
        """report batch pool errors"""
        errors = []
        pool = self.batch_client.pool.get(self.pool_id)
        if pool.resize_errors:
            for e in pool.resize_errors:
                err_dict = {"code": e.code, "message": e.message}
                errors.append(err_dict)
            self.report_job_error(job, msg=f"Batch pool error: {e}")

    def _report_task_status(self, job: SubmittedJobInfo):
        """report batch task status. Return True if still running, False if not"""
        try:
            task: bm.CloudTask = self.batch_client.task.get(
                job_id=self.job_id, task_id=job.external_jobid
            )
        except Exception as e:
            self.logger.warning(f"Unable to get Azure Batch Task status: {e}")
            return True

        self.logger.debug(
            f"task {task.id}: "
            f"creation_time={task.creation_time} "
            f"state={task.state} node_info={task.node_info}\n"
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
                else:
                    msg = f"\nUnknown task execution result: {ei.__dict__}\n"
                    self.report_job_error(
                        job,
                        msg=msg,
                        stderr=stderr,
                        stdout=stdout,
                    )
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
                    stderr_stream = self._read_stream_as_string(stderr_file, "utf-8")
                except Exception:
                    stderr_stream = ""

                try:
                    stdout_file = self.batch_client.file.get_from_compute_node(
                        self.pool_id, n.id, "/startup/stdout.txt"
                    )
                    stdout_stream = self._read_stream_as_string(stdout_file, "utf-8")
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
        """Creates a pool of compute nodes"""

        image_ref = ImageReference(
            publisher=self.settings.pool_image_publisher,
            offer=self.settings.pool_image_offer,
            sku=self.settings.pool_image_sku,
            version="latest",
        )

        network_config = None
        if self.settings.pool_subnet_id is not None:
            network_config = NetworkConfiguration(
                subnet_id=self.settings.pool_subnet_id
            )

        # configure batch pool identity
        batch_pool_identity = None
        if self.settings.managed_identity_resource_id is not None:
            batch_pool_identity = BatchPoolIdentity(
                type=PoolIdentityType.USER_ASSIGNED,
                user_assigned_identities={
                    self.settings.managed_identity_resource_id: UserAssignedIdentities()
                },
            )

        # configure a container registry
        # Specify container configuration, fetching an image
        #  https://docs.microsoft.com/en-us/azure/batch/batch-docker-container-workloads#prefetch-images-for-container-configuration
        container_config = ContainerConfiguration(
            type="dockerCompatible",
            container_image_names=[self.container_image],
        )

        user = None
        passw = None
        identity_ref = None
        registry_conf = None

        if self.settings.container_registry_url is not None:
            if (
                self.settings.container_registry_user is not None
                and self.settings.container_registry_pass is not None
            ):
                user = self.settings.container_registry_user
                passw = self.settings.container_registry_pass
            elif self.settings.managed_identity_resource_id is not None:
                identity_ref = ComputeNodeIdentityReference(
                    resource_id=self.settings.managed_identity_resource_id
                )
            else:
                raise WorkflowError(
                    "No container registry authentication scheme set. Please set the "
                    "BATCH_CONTAINER_REGISTRY_USER and BATCH_CONTAINER_REGISTRY_PASS "
                    "or set MANAGED_IDENTITY_CLIENT_ID and "
                    "MANAGED_IDENTITY_RESOURCE_ID."
                )

            registry_conf = [
                ContainerRegistry(
                    registry_server=self.settings.container_registry_url,
                    identity_reference=identity_ref,
                    user_name=str(user),
                    password=str(passw),
                )
            ]

            # Specify container configuration, fetching an image
            #  https://docs.microsoft.com/en-us/azure/batch/batch-docker-container-workloads#prefetch-images-for-container-configuration
            container_config = ContainerConfiguration(
                type=ContainerType.DOCKER_COMPATIBLE,
                container_image_names=[self.container_image],
                container_registries=registry_conf,
            )

        # default to no start task
        start_task_conf = None

        # if configured use start task bash script from url
        # can be SAS url or other accessible url hosting bash script
        if self.settings.node_start_task_url is not None:
            _SIMPLE_TASK_NAME = "start_task.sh"
            start_task_admin = UserIdentity(
                auto_user=AutoUserSpecification(
                    elevation_level=ElevationLevel.ADMIN,
                    scope=AutoUserScope.POOL,
                )
            )
            start_task_conf = StartTask(
                command_line=f"bash {_SIMPLE_TASK_NAME}",
                resource_files=[
                    ResourceFile(
                        file_path=_SIMPLE_TASK_NAME,
                        http_url=self.settings.node_start_task_url,
                    )
                ],
                user_identity=start_task_admin,
            )

        # auto scale requires the initial dedicated node count to be zero
        # min allowed interval of five minutes
        if self.settings.autoscale:
            self.settings.pool_node_count = 0
            scale_settings = ScaleSettings(
                auto_scale=AutoScaleSettings(
                    formula=DEFAULT_AUTO_SCALE_FORMULA,
                    evaluation_interval=datetime.timedelta(minutes=5),
                )
            )

        scale_settings = ScaleSettings(
            fixed_scale=FixedScaleSettings(
                target_dedicated_nodes=self.settings.pool_node_count
            )
        )

        pool_params = Pool(
            identity=batch_pool_identity,
            display_name=self.pool_id,
            vm_size=self.settings.pool_vm_size,
            deployment_configuration=DeploymentConfiguration(
                virtual_machine_configuration=VirtualMachineConfiguration(
                    image_reference=image_ref,
                    container_configuration=container_config,
                    node_agent_sku_id=self.settings.pool_vm_node_agent_sku_id,
                ),
            ),
            scale_settings=scale_settings,
            start_task=start_task_conf,
            network_configuration=network_config,
            task_slots_per_node=self.settings.tasks_per_node,
            task_scheduling_policy=TaskSchedulingPolicy(
                node_fill_type=self.settings.node_fill_type
            ),
            target_node_communication_mode=NodeCommunicationMode.CLASSIC,
        )
        try:
            self.logger.info(f"Creating pool: {self.pool_id}")
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
        """Creates a job with the specified ID, associated with the specified pool"""

        self.logger.info(f"Creating batch job {self.job_id}")

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
