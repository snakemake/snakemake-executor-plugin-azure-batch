import os
from typing import Optional
import snakemake.common.tests
from snakemake_interface_executor_plugins import ExecutorSettingsBase

from snakemake_executor_plugin_azure_batch import ExecutorSettings


class TestWorkflowsBase(snakemake.common.tests.TestWorkflowsBase):
    __test__ = True

    def get_executor(self) -> str:
        return "azure-batch"

    def get_executor_settings(self) -> Optional[ExecutorSettingsBase]:
        # instatiate ExecutorSettings of this plugin as appropriate
        return ExecutorSettings(
            account_url=os.getenv("AZ_BATCH_ACCOUNT_URL"),
            account_key=os.getenv("AZ_BATCH_KEY"),
        )

    def get_default_storage_provider(self) -> Optional[str]:
        # Return name of default remote provider if required for testing,
        # otherwise None.
        return "AzBlob"

    def get_default_storage_prefix(self) -> Optional[str]:
        # Return default remote prefix if required for testing,
        # otherwise None.
        return os.getenv("AZ_BLOB_PREFIX")

    def get_envvars(self):
        # envvars that will be passed to the batch tasks
        return ["AZ_BLOB_ACCOUNT_URL", "AZ_BLOB_CREDENTIAL"]
