import os
from typing import Optional

import snakemake.common.tests
from snakemake_interface_executor_plugins.settings import ExecutorSettingsBase

from snakemake_executor_plugin_azure_batch import ExecutorSettings


class TestWorkflowsBase(snakemake.common.tests.TestWorkflowsMinioPlayStorageBase):
    def get_executor(self) -> str:
        return "azure-batch"

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        # instatiate ExecutorSettings of this plugin as appropriate
        return ExecutorSettings(
            account_url=os.getenv("SNAKEMAKE_AZURE_BATCH_ACCOUNT_URL"),
            account_key=os.getenv("SNAKEMAKE_AZURE_BATCH_ACCOUNT_KEY"),
        )

    def get_assume_shared_fs(self) -> bool:
        return False

    def get_remote_execution_settings(
        self,
    ) -> snakemake.settings.RemoteExecutionSettings:
        return snakemake.settings.RemoteExecutionSettings(
            seconds_between_status_checks=5,
            envvars=self.get_envvars(),
            # TODO remove once we have switched to stable snakemake for dev
            container_image="snakemake/snakemake:latest",
        )
