from unittest.mock import AsyncMock, MagicMock, patch
from tests import TestWorkflowsBase

from azure.batch.models import CloudTask, TaskState


class TestWorkflowsMocked(TestWorkflowsBase):
    __test__ = True

    @patch(
        "azure.batch.operations.taskoperations.TaskOperations.add",
        new=MagicMock(autospec=True),
    )
    @patch(
        "azure.batch.operations.taskoperations.TaskOperations.get",
        new=MagicMock(
            return_value=CloudTask(state=TaskState.completed),
            autospec=True,
        ),
    )
    @patch(
        "snakemake.dag.DAG.check_and_touch_output",
        new=AsyncMock(autospec=True),
    )
    @patch(
        "snakemake_storage_plugin_s3.StorageObject.managed_size",
        new=AsyncMock(autospec=True, return_value=0),
    )
    @patch(
        # mocking has to happen in the importing module, see
        # http://www.gregreda.com/2021/06/28/mocking-imported-module-function-python
        "snakemake.jobs.wait_for_files",
        new=AsyncMock(autospec=True),
    )
    def run_workflow(self, *args, **kwargs):
        super().run_workflow(*args, **kwargs)
