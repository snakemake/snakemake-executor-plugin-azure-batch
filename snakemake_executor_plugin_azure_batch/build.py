import datetime
import uuid

import azure.batch.models as bm
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
from snakemake_interface_executor_plugins.jobs import JobExecutorInterface

from snakemake_executor_plugin_azure_batch.constant import DEFAULT_AUTO_SCALE_FORMULA


def batch_pool_params(pool_id: str, settings, container_image: str) -> Pool:
    """
    Constructs a Batch Pool object with the specified parameters.

    Args:
        pool_id (str): The ID of the pool.
        settings (ExecutorSettings): The settings for the executor.
        container_image (str): The name of the container image.

    Returns:
        Pool: The constructed Batch Pool object.
    """
    image_ref = ImageReference(
        publisher=settings.pool_image_publisher,
        offer=settings.pool_image_offer,
        sku=settings.pool_image_sku,
        version="latest",
    )

    network_config = None
    if settings.pool_subnet_id is not None:
        network_config = NetworkConfiguration(subnet_id=settings.pool_subnet_id)

    # configure batch pool identity
    batch_pool_identity = None
    if settings.managed_identity_resource_id is not None:
        batch_pool_identity = BatchPoolIdentity(
            type=PoolIdentityType.USER_ASSIGNED,
            user_assigned_identities={
                settings.managed_identity_resource_id: UserAssignedIdentities()
            },
        )

    # configure a container registry
    # Specify container configuration, fetching an image
    #  https://docs.microsoft.com/en-us/azure/batch/batch-docker-container-workloads#prefetch-images-for-container-configuration
    container_config = ContainerConfiguration(
        type="dockerCompatible",
        container_image_names=[container_image],
    )

    user = None
    passw = None
    identity_ref = None
    registry_conf = None

    if settings.container_registry_url is not None:
        if (
            settings.container_registry_user is not None
            and settings.container_registry_pass is not None
        ):
            user = settings.container_registry_user
            passw = settings.container_registry_pass
        elif settings.managed_identity_resource_id is not None:
            identity_ref = ComputeNodeIdentityReference(
                resource_id=settings.managed_identity_resource_id
            )
        else:
            raise WorkflowError(
                "No container registry authentication scheme set. Please set the "
                "SNAKEMAKE_AZURE_BATCH_CONTAINER_REGISTRY_USER and "
                "SNAKEMAKE_AZURE_BATCH_CONTAINER_REGISTRY_PASS "
                "or set SNAKEMAKE_AZURE_BATCH_MANAGED_IDENTITY_CLIENT_ID and "
                "SNAKEMAKE_AZURE_BATCH_MANAGED_IDENTITY_RESOURCE_ID "
                "and Grant it permissions to the Azure Container Registry."
            )

        registry_conf = [
            ContainerRegistry(
                registry_server=settings.container_registry_url,
                identity_reference=identity_ref,
                user_name=str(user),
                password=str(passw),
            )
        ]

        # Specify container configuration, fetching an image
        #  https://docs.microsoft.com/en-us/azure/batch/batch-docker-container-workloads#prefetch-images-for-container-configuration
        container_config = ContainerConfiguration(
            type=ContainerType.DOCKER_COMPATIBLE,
            container_image_names=[container_image],
            container_registries=registry_conf,
        )

    # default to no start task
    start_task_conf = None

    # if configured use start task bash script from url
    # can be SAS url or other accessible url hosting bash script
    if settings.node_start_task_url is not None:
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
                    http_url=settings.node_start_task_url,
                )
            ],
            user_identity=start_task_admin,
        )

    # auto scale requires the initial dedicated node count to be zero
    # min allowed interval of five minutes
    if settings.autoscale:
        settings.pool_node_count = 0
        scale_settings = ScaleSettings(
            auto_scale=AutoScaleSettings(
                formula=DEFAULT_AUTO_SCALE_FORMULA,
                evaluation_interval=datetime.timedelta(minutes=5),
            )
        )

    scale_settings = ScaleSettings(
        fixed_scale=FixedScaleSettings(target_dedicated_nodes=settings.pool_node_count)
    )

    return Pool(
        identity=batch_pool_identity,
        display_name=pool_id,
        vm_size=settings.pool_vm_size,
        deployment_configuration=DeploymentConfiguration(
            virtual_machine_configuration=VirtualMachineConfiguration(
                image_reference=image_ref,
                container_configuration=container_config,
                node_agent_sku_id=settings.pool_vm_node_agent_sku_id,
            ),
        ),
        scale_settings=scale_settings,
        start_task=start_task_conf,
        network_configuration=network_config,
        task_slots_per_node=settings.tasks_per_node,
        task_scheduling_policy=TaskSchedulingPolicy(
            node_fill_type=settings.node_fill_type
        ),
        target_node_communication_mode=NodeCommunicationMode.CLASSIC,
    )


def batch_task(
    job: JobExecutorInterface,
    container_image: str,
    envvars: dict,
    remote_task_command: str,
) -> bm.TaskAddParameter:
    """
    Creates a batch task for executing a remote task in Azure Batch.

    Args:
        job (JobExecutorInterface): The job executor interface.
        container_image (str): The name of the container image to use for the task.
        envvars (dict): A dictionary of environment variables to set for the task.
        remote_task_command (str): The command to execute in the remote task.

    Returns:
        bm.TaskAddParameter: The batch task object.

    """
    task_uuid = str(uuid.uuid4())
    task_id = f"{job.name}-{task_uuid}"

    env_settings = []
    for key, value in envvars.items():
        env_settings.append(bm.EnvironmentSetting(name=key, value=value))

    return bm.TaskAddParameter(
        id=task_id,
        command_line=remote_task_command,
        display_name=job.name,
        user_identity=bm.UserIdentity(
            auto_user=bm.AutoUserSpecification(
                elevation_level=bm.ElevationLevel.admin,
                scope=bm.AutoUserScope.pool,
            )
        ),
        container_settings=bm.TaskContainerSettings(
            image_name=container_image,
            container_run_options="--rm",
        ),
        environment_settings=env_settings,
    )
