import os
from typing import Optional
import snakemake.common.tests
from snakemake_interface_executor_plugins import ExecutorSettingsBase

from snakemake_executor_plugin_azure_batch import ExecutorSettings


class TestWorkflowsBase(snakemake.common.tests.TestWorkflowsMinioPlayStorageBase):
    __test__ = True

    def get_executor(self) -> str:
        return "azure-batch"

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        # instatiate ExecutorSettings of this plugin as appropriate
        return ExecutorSettings(
            account_url=os.getenv("AZ_BATCH_ACCOUNT_URL"),
            account_key=os.getenv("AZ_BATCH_KEY"),
        )
