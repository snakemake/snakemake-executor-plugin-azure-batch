from unittest.mock import AsyncMock, MagicMock, patch

from azure.batch.models import (
    CloudTask,
    TaskExecutionInformation,
    TaskExecutionResult,
    TaskState,
)

from tests import TestWorkflowsBase


class TestWorkflowsMocked(TestWorkflowsBase):
    __test__ = True

    @patch(
        "azure.batch.operations.TaskOperations.add",
        new=MagicMock(autospec=True),
    )
    @patch(
        "azure.batch.operations.TaskOperations.get",
        new=MagicMock(
            autospec=True,
            return_value=CloudTask(
                state=TaskState.completed,
                execution_info=TaskExecutionInformation(
                    result=TaskExecutionResult.success,
                    retry_count=0,  # necessary inits
                    requeue_count=0,
                ),
            ),
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
