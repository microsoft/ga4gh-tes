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
        private const string NodeTaskRunnerFilename = "tes-runner";
        private const string VMPerformanceArchiverFilename = "tes_vm_perf.tar.gz";
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
            var batchRunCommand = $"/bin/bash -c \"{BatchNodeScriptBuilder.CreateWgetDownloadCommand(batchScriptAssets.BatchScriptUrl, $"${BatchNodeScriptBuilder.BatchTaskDirEnvVarName}/{batchScriptAssets.BatchScriptFileName}", setExecutable: true)} && ${BatchNodeScriptBuilder.BatchTaskDirEnvVarName}/{batchScriptAssets.BatchScriptFileName}\"";

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

            var nodeTaskRunnerUrl = await storageAccessProvider.GetInternalTesBlobUrlAsync(NodeTaskRunnerFilename, cancellationToken);
            var nodeVMPerfArchiveUrl = await storageAccessProvider.GetInternalTesBlobUrlAsync(VMPerformanceArchiverFilename, cancellationToken);

            var batchNodeScript = batchNodeScriptBuilder
                .WithAlpineWgetInstallation()
                .WithMetrics()
                .WithRunnerFilesDownloadUsingWget(nodeTaskRunnerUrl, nodeTaskUrl, nodeVMPerfArchiveUrl)
                .WithExecuteRunner()
                .WithLocalRuntimeSystemInformation()
                .Build();

            // Python command to extract the log destination from the runner URL
            string pythonCommand = $@"
get_log_destination_from_runner_url() {{
python3 <<EOF
nodeTaskUrl = '{nodeTaskUrl}'
from urllib.parse import urlparse, urlunparse
url_parts = urlparse(nodeTaskUrl)
container_path = url_parts.path
if container_path.endswith('/') == False:
    path_segment = container_path.split('/')
    container_path = '/'.join(path_segment[:-1]) + '/'
container_path = container_path
new_url = urlunparse((url_parts.scheme, url_parts.netloc, container_path, '', '', ''))
print(new_url)
EOF
}}
";

            // Write out the entire bash script with variable substitution turned on
            batchNodeScript = $@"#!/bin/bash
set -x
batch_script_task(){{
{batchNodeScript}
}}
{pythonCommand}

# Upload the stdout and stderr logs:
upload_logs() {{
    LOGGING_URL=$(get_log_destination_from_runner_url)
    
    echo ""Uploading logs to storage account: $LOG_URL""
    export AZCOPY_AUTO_LOGIN_TYPE=MSI
    do_upload stdout.txt ""$AZ_BATCH_TASK_DIR"" ""$LOGGING_URL""
    do_upload stderr.txt ""$AZ_BATCH_TASK_DIR"" ""$LOGGING_URL""
}}
do_upload() {{
    local file_name=${{2}}/${{1}}
    echo ""Uploadings $1 to $3""
    if [ -f ""$1"" ]; then
        touch ""$1""
    fi
    cp ""$file_name"" ""${{file_name}}.snap""
    azcopy copy --log-level=ERROR --output-level essential ""${{file_name}}.snap"" ""${{3}}${{1}}""
}}

# Trap helper functions:
on_error() {{
    trap - ERR EXIT
    if [ -f ""$AZ_BATCH_TASK_DIR/exit_code.txt"" ]; then
        EXIT_CODE=$(cat ""$AZ_BATCH_TASK_DIR/exit_code.txt"")
        echo ""Return code trapped from exit code file: $EXIT_CODE""
    fi
    echo ""Error was trapped during batch_script execution""
    echo ""Current directory: $(pwd)""
    echo ""Environment: $(env)""
    echo ""Exports in the current shell: $(export -p)""
}}

# Run the trask and attempt to capture any errors:
trap 'echo \\$? > ""$AZ_BATCH_TASK_DIR/exit_code.txt""; on_error; upload_logs' ERR EXIT
batch_script_task

# Capture the exit code and upload the log files
bath_script_return_code=$?
trap - ERR EXIT
if [ -f ""$AZ_BATCH_TASK_DIR/exit_code.txt"" ]; then
    EXIT_CODE=$(cat ""$AZ_BATCH_TASK_DIR/exit_code.txt"")
    echo ""Return code trapped from exit code file: $EXIT_CODE""
    on_error
    upload_logs
else
    EXIT_CODE=$bath_script_return_code
    echo ""Return code: $EXIT_CODE""
    upload_logs
fi

# Return the exit code
echo ""Exiting with code: $EXIT_CODE""
echo Task complete
exit $EXIT_CODE
";
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
