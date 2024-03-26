trap 'cleanup_task $?' ERR

cleanup_task() {
    ../clean-executor.sh; exit $1
}
{CleanupScriptLines}
chmod u+x ../clean-executor.sh
{GetBatchScriptFile}
/bin/sh {BatchScriptPath}
cleanup_task 0