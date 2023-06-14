trap 'cleanup_task $?' ERR
cleanup_task() {
    ../clean-executor.sh; exit $1
}
{CleanupScriptLines}
chmod u+x ./tRunner
chmod u+x ../clean-executor.sh
/bin/sh {BatchScriptPath}
cleanup_task 0