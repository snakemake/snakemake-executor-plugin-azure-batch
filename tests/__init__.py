import os
from typing import Optional

import snakemake.common.tests
from snakemake_interface_executor_plugins.settings import ExecutorSettingsBase

from snakemake_executor_plugin_azure_batch import ExecutorSettings


class TestWorkflowsBase(snakemake.common.tests.TestWorkflowsMinioPlayStorageBase):
    def get_executor(self) -> str:
        return "azure-batch"

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        # instantiate ExecutorSettings of this plugin as appropriate
        return ExecutorSettings(
            account_url=os.getenv("SNAKEMAKE_AZURE_BATCH_ACCOUNT_URL"),
            subscription_id=os.getenv("SNAKEMAKE_AZURE_BATCH_SUBSCRIPTION_ID"),
            resource_group_name=os.getenv("SNAKEMAKE_AZURE_BATCH_RESOURCE_GROUP_NAME"),
        )

    def get_assume_shared_fs(self) -> bool:
        return False

    def get_remote_execution_settings(
        self,
    ) -> snakemake.settings.types.RemoteExecutionSettings:
        return snakemake.settings.types.RemoteExecutionSettings(
            seconds_between_status_checks=5,
            envvars=self.get_envvars(),
            container_image="snakemake/snakemake:latest",
        )
