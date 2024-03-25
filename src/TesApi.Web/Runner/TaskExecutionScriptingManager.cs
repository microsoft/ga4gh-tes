// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Tes.Models;
using TesApi.Web.Storage;

namespace TesApi.Web.Runner
{
    /// <summary>
    /// Manages the creation and uploading of the Batch script to execute and all its dependencies
    /// </summary>
    public class TaskExecutionScriptingManager
    {
        private const string NodeTaskFilename = "runner-task.json";
        private const string VMPerformanceArchiverFilename = "tes_vm_monitor.tar.gz";
        private const string BatchScriptFileName = "batch_script";

        private readonly IStorageAccessProvider storageAccessProvider;
        private readonly TaskToNodeTaskConverter taskToNodeConverter;
        private readonly ILogger<TaskExecutionScriptingManager> logger;
        private readonly BatchNodeScriptBuilder batchNodeScriptBuilder;

        /// <summary>
        /// Constructor of TaskExecutionScriptingManager
        /// </summary>
        /// <param name="storageAccessProvider"></param>
        /// <param name="taskToNodeConverter"></param>
        /// <param name="batchNodeScriptBuilder"></param>
        /// <param name="logger"></param>
        public TaskExecutionScriptingManager(IStorageAccessProvider storageAccessProvider, TaskToNodeTaskConverter taskToNodeConverter, BatchNodeScriptBuilder batchNodeScriptBuilder, ILogger<TaskExecutionScriptingManager> logger)
        {
            ArgumentNullException.ThrowIfNull(storageAccessProvider);
            ArgumentNullException.ThrowIfNull(taskToNodeConverter);
            ArgumentNullException.ThrowIfNull(batchNodeScriptBuilder);
            ArgumentNullException.ThrowIfNull(logger);

            this.storageAccessProvider = storageAccessProvider;
            this.taskToNodeConverter = taskToNodeConverter;
            this.batchNodeScriptBuilder = batchNodeScriptBuilder;
            this.logger = logger;
        }

        /// <summary>
        /// Prepares the runtime scripting assets required for the execution of a TES task in a Batch node using the TES runner. 
        /// </summary>
        /// <param name="tesTask"></param>
        /// <param name="nodeTaskConversionOptions"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<BatchScriptAssetsInfo> PrepareBatchScriptAsync(TesTask tesTask, NodeTaskConversionOptions nodeTaskConversionOptions, CancellationToken cancellationToken)
        {
            try
            {
                await TryUploadServerTesTask(tesTask, cancellationToken);

                var nodeTaskUrl = await CreateAndUploadNodeTaskAsync(tesTask, nodeTaskConversionOptions, cancellationToken);

                var batchScriptUrl = await CreateAndUploadBatchScriptAsync(tesTask, nodeTaskUrl, cancellationToken);

                return new BatchScriptAssetsInfo(batchScriptUrl, nodeTaskUrl, BatchScriptFileName);
            }
            catch (Exception e)
            {
                logger.LogError(e,
                    "Failed to perform the preparation steps for the execution of the task on the Batch node");
                throw;
            }
        }

        private async Task TryUploadServerTesTask(TesTask tesTask, CancellationToken cancellationToken)
        {
            try
            {
                var severTesTaskContent = JsonConvert.SerializeObject(tesTask,
                    new JsonSerializerSettings
                    {
                        NullValueHandling = NullValueHandling.Ignore,
                        DefaultValueHandling = DefaultValueHandling.Ignore
                    });

                await UploadContentAsBlobToInternalTesLocationAsync(tesTask, severTesTaskContent, "server-tes-task.json",
                    cancellationToken);
            }
            catch (Exception e)
            {
                //we are not bubbling up the exception as it is not critical for the execution of the task
                // and just in case the task may have values that are not serializable. We can revisit this assumption later and throw
                logger.LogError(e,
                    "Failed to upload the server TesTask to the internal Tes location");
            }
        }

        /// <summary>
        /// Creates the run command the Batch node must run to execute the task
        /// </summary>
        /// <param name="batchScriptAssets"></param>
        /// <returns></returns>
        public string ParseBatchRunCommand(BatchScriptAssetsInfo batchScriptAssets)
        {
            var batchRunCommand = $"/bin/bash -c \"{BatchScheduler.CreateWgetDownloadCommand(batchScriptAssets.BatchScriptUrl, $"${BatchNodeScriptBuilder.BatchTaskDirEnvVarName}/{batchScriptAssets.BatchScriptFileName}", setExecutable: true)} && ${BatchNodeScriptBuilder.BatchTaskDirEnvVarName}/{batchScriptAssets.BatchScriptFileName}\"";

            // Replace any URL query strings with the word REMOVED
            var sanitizedLogEntry = RemoveQueryStringsFromText(batchRunCommand);

            logger.LogInformation("Run command (sanitized): " + sanitizedLogEntry);

            return batchRunCommand;
        }

        private static string RemoveQueryStringsFromText(string batchRunCommand)
        {
            const string pattern = @"(https?:\/\/[^?\s]+)\?[^?\s]*";
            const string replacement = "$1?REMOVED";
            string sanitizedLogEntry = Regex.Replace(batchRunCommand, pattern, replacement);
            return sanitizedLogEntry;
        }

        private async Task<Uri> CreateAndUploadBatchScriptAsync(TesTask tesTask, Uri nodeTaskUrl, CancellationToken cancellationToken)
        {
            logger.LogInformation($"Creating and uploading Batch script for Task ID: {tesTask.Id}");

            var nodeVMPerfArchiveUrl = await storageAccessProvider.GetInternalTesBlobUrlAsync(VMPerformanceArchiverFilename, cancellationToken);
            var batchScriptLoggerUrl = storageAccessProvider.GetInternalTesTaskBlobUrlWithoutSasToken(tesTask, "");

            var batchNodeScript = batchNodeScriptBuilder
                .WithAlpineWgetInstallation()
                .WithMetrics()
                .WithRunnerTaskDownloadUsingWget(nodeTaskUrl, nodeVMPerfArchiveUrl)
                .WithExecuteRunner()
                .WithLocalRuntimeSystemInformation()
                .Build();

            // TODO: add Terra check from TerraOptionsAreConfigured
            // Outside the Terra environment:
            //  - MSI is available, upload is performed with azcopy using MSI auth
            //  - if more than one MSI is available, the MSI client ID must be specified
            // Inside the Terra environment:
            //  - TODO: upload could be done with tes-runner and an auto-generated runner-task.json just for logs (outputs are just stdout and stderr)
            //  - for the moment with no MSI uploads are not done
            bool MSIAvailable = true;
            string MSIAvailableStr = MSIAvailable.ToString().ToLower();
            // On VMs with more than one MSI you must specify the client ID of the MSI you want to use
            // On VMs with a single MSI (the default CoA MSI) you can leave the MSIClientID as an empty string
            string MSIClientID = "";

            // Write out the entire bash script with variable substitution turned on
            // print_debug_info and upload_logs are called to capture stderr, stdout, and some env information on all running tasks
            // This is especially useful when tes-runner fails to produce any output
            batchNodeScript = $@"#!/bin/bash
# set -x will print each command before it is executed
set -x
batch_script_task(){{
# set -e will cause any error to exit the script
set -e

{batchNodeScript}

set +e
}}

# Upload the stdout and stderr logs (a snapshot is uploaded to avoid azcopy errors)
# Azcopy will authenticate using the MSI (managed service identity)
UPLOAD_LOGS_DONE=false
upload_logs() {{
    if [ ""$UPLOAD_LOGS_DONE"" = true ]; then
        echo ""Warning: upload_logs was already called once, running again""
    fi
    LOGGING_URL=""{batchScriptLoggerUrl}""
    if [[ ""${{LOGGING_URL: -1}}"" != ""/"" ]]; then
        LOGGING_URL+=""/""
    fi

    echo ""Uploading logs to storage account: $LOGGING_URL""
    export AZCOPY_AUTO_LOGIN_TYPE=MSI
    MSIClientID=""{MSIClientID}""
    if [[ -n ""$MSIClientID"" ]]; then
        export AZCOPY_MSI_CLIENT_ID=""$MSIClientID""
        echo ""Using MSI with client ID: $AZCOPY_MSI_CLIENT_ID""
    fi
    if [[ ""{MSIAvailableStr}"" == ""true"" ]]; then
        prep_logfile stdout.txt ""$AZ_BATCH_TASK_DIR"" ""$LOGGING_URL""
        prep_logfile stderr.txt ""$AZ_BATCH_TASK_DIR"" ""$LOGGING_URL""
        azcopy copy --skip-version-check --log-level=ERROR --output-level essential ""${{AZ_BATCH_TASK_DIR}}/*.snap.txt"" ""$LOGGING_URL""
        UPLOAD_LOGS_DONE=true
    fi
}}

# Take a snapshot of the log file to avoid azcopy errors
prep_logfile() {{
    local file_name=""${{2}}/${{1}}""
    local new_file_name=""${{file_name%.txt}}.snap.txt""
    echo ""Uploading $1 to $3""
    if [ ! -f ""$file_name"" ]; then
        touch ""$file_name""
    fi
    cp ""$file_name"" ""$new_file_name""
}}

# Trap helper functions, this will log/run only on a trap trigger
on_error() {{
    trap - ERR EXIT
    if [ -f ""$AZ_BATCH_TASK_DIR/exit_code.txt"" ]; then
        EXIT_CODE=$(cat ""$AZ_BATCH_TASK_DIR/exit_code.txt"")
        echo ""Return code trapped from exit code file: $EXIT_CODE""
    fi
    echo ""Error was trapped during batch_script execution""
}}

# Print debugging information that may be useful for troubleshooting
print_debug_info(){{
    # Save current state of 'set -x'
    [[ $- = *x* ]] && XTRACE_SETTING=""on"" || XTRACE_SETTING=""off""
    # Turn off 'set -x'
    set +x

    print_heading(){{
        echo ""----------------------------------------""
        echo ""  $1""
        echo ""----------------------------------------""
    }}

    echo ""Current directory: $(pwd)""
    print_heading ""System info:""
    uname -a || true
    print_heading ""System release:""
    lsb_release -a || true
    print_heading ""Current running processes:""
    ps -ef || true
    print_heading ""Environment:""
    env || true
    print_heading ""Exports in the current shell:""
    export -p || true
    print_heading ""Listing files in the AZ_BATCH_TASK_DIR directory: [\""$AZ_BATCH_TASK_DIR\""]""
    ls -R -la ""$AZ_BATCH_TASK_DIR"" || true
    print_heading ""Listing files in the current directory: [\""$(pwd)\""]""
    ls -R -la || true
    print_heading ""Disk usage:""
    df -h || true
    print_heading ""Memory usage:""
    free -m || true
    print_heading ""CPU info:""
    lscpu || true
    print_heading ""Docker processes:""
    docker ps -a || true
    print_heading ""Docker images:""
    docker images || true
    print_heading ""Docker system info:""
    docker system info || true
    print_heading ""Docker system df:""
    docker system df || true
    print_heading ""Listing files in the /mnt/docker directory:""
    ls -R -la /mnt/docker || true
    print_heading ""dmesg out of memory:""
    dmesg | grep -i 'out of memory' || true
    print_heading ""Last 200 lines of jounralctl:""
    journalctl -n 200 --no-pager --no-hostname -m || true

    # Restore 'set -x' to its original state
    if [[ $XTRACE_SETTING = ""on"" ]]; then
        set -x
    fi
}}

# Run the task and attempt to capture any errors:
trap 'echo \\$? > ""$AZ_BATCH_TASK_DIR/exit_code.txt""; on_error; upload_logs' ERR EXIT
batch_script_task

# Capture the exit code and upload the log files
bath_script_return_code=$?
trap - ERR EXIT
if [ -f ""$AZ_BATCH_TASK_DIR/exit_code.txt"" ] && [ ! ""$UPLOAD_LOGS_DONE"" ]; then
    EXIT_CODE=$(cat ""$AZ_BATCH_TASK_DIR/exit_code.txt"")
    echo ""Return code trapped from exit code file: $EXIT_CODE""
    on_error

    # Upload only if an error occurred
    if [[ ""{MSIAvailableStr}"" == ""true"" ]]; then
        print_debug_info
        upload_logs
    fi
else
    EXIT_CODE=$bath_script_return_code
    echo ""Return code: $EXIT_CODE""
fi

# Always capture the state if we can upload:
# if [[ ""{MSIAvailableStr}"" == ""true"" ]]; then
#     print_debug_info
#     upload_logs
# fi

# Return the exit code
trap - ERR EXIT
echo ""Exiting with code: $EXIT_CODE""
echo Task complete
exit $EXIT_CODE
";
            // Remove any accidental windows line endings, bash doesn't like them
            batchNodeScript = batchNodeScript.Replace("\r\n", "\n");

            var batchNodeScriptUrl = await UploadContentAsBlobToInternalTesLocationAsync(tesTask, batchNodeScript, BatchScriptFileName, cancellationToken);

            logger.LogInformation($"Successfully created and uploaded Batch script for Task ID: {tesTask.Id}");

            return batchNodeScriptUrl;
        }

        private async Task<Uri> CreateAndUploadNodeTaskAsync(TesTask tesTask, NodeTaskConversionOptions nodeTaskConversionOptions, CancellationToken cancellationToken)
        {
            logger.LogInformation($"Creating and uploading node task definition file for Task ID: {tesTask.Id}");

            var nodeTask = await taskToNodeConverter.ToNodeTaskAsync(tesTask, nodeTaskConversionOptions, cancellationToken);

            var nodeTaskContent = JsonConvert.SerializeObject(nodeTask,
                new JsonSerializerSettings
                {
                    NullValueHandling = NullValueHandling.Ignore,
                    DefaultValueHandling = DefaultValueHandling.Ignore
                });

            var nodeTaskUrl = await UploadContentAsBlobToInternalTesLocationAsync(tesTask, nodeTaskContent, NodeTaskFilename, cancellationToken);

            logger.LogInformation($"Successfully created and uploaded node task definition file for Task ID: {tesTask.Id}");

            return nodeTaskUrl;
        }

        private async Task<Uri> UploadContentAsBlobToInternalTesLocationAsync(TesTask tesTask,
            string content, string fileName, CancellationToken cancellationToken)
        {
            var blobUrl =
                await storageAccessProvider.GetInternalTesTaskBlobUrlAsync(tesTask, fileName, cancellationToken);

            await storageAccessProvider.UploadBlobAsync(blobUrl, content, cancellationToken);
            return blobUrl;
        }
    }

    /// <summary>
    /// Contains information of the scripting assets required for the execution of a TES task in a Batch node using the TES runner.
    /// </summary>
    /// <param name="BatchScriptUrl"></param>
    /// <param name="NodeTaskUrl"></param>
    /// <param name="BatchScriptFileName"></param>
    public record BatchScriptAssetsInfo(Uri BatchScriptUrl, Uri NodeTaskUrl, string BatchScriptFileName);
}
