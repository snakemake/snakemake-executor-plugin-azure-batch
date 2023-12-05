AZURE_BATCH_RESOURCE_ENDPOINT = "https://batch.core.windows.net/"

DEFAULT_AUTO_SCALE_FORMULA = (
    "$samples = $PendingTasks.GetSamplePercent(TimeInterval_Minute * 5);"  # noqa
    "$tasks = $samples < 70 ? max(0,$PendingTasks.GetSample(1)) : max( $PendingTasks.GetSample(1), avg($PendingTasks.GetSample(TimeInterval_Minute * 5)));"  # noqa
    "$targetVMs = $tasks > 0? $tasks:max(0, $TargetDedicatedNodes/2);"
    "$TargetDedicatedNodes = max(0, min($targetVMs, 10));"
    "$NodeDeallocationOption = taskcompletion;"
)
