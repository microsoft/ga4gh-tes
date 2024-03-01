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


            
            // Wrap the batch_script so we can call it with logging in screen:
            /*
            batchNodeScript = "#!/bin/bash\nbatch_script_task(){\nlocal task_dir=$1\nsource $task_dir/batch_script_env.sh\n" + batchNodeScript;
            batchNodeScript += "\n}\n";
            batchNodeScript += "export -f batch_script_task\n";
            batchNodeScript += "rm -f \"$AZ_BATCH_TASK_DIR/batch_script_log.txt\"\n";
            */
            string pythonCommand = $@"#!/bin/bash
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
            pythonCommand = pythonCommand.Replace("\r\n", "\n");
            /*
            batchNodeScript += pythonCommand;
            batchNodeScript += "LOG_URL=$(get_log_destination_from_runner_url)\n";
            batchNodeScript += "echo $LOG_URL\n";
            batchNodeScript += "export -p > $AZ_BATCH_TASK_DIR/batch_script_env.sh\n";
            batchNodeScript += "set > $AZ_BATCH_TASK_DIR/batch_script_env.sh\n";
            batchNodeScript += "# Run the batch_script_task in a screen session, and capture the exit code (with trap)\n";
            batchNodeScript += "trap 'echo \\$? > $AZ_BATCH_TASK_DIR/exit_code.txt' EXIT; batch_script_task \"$AZ_BATCH_TASK_DIR\"n";
            batchNodeScript += "screen -L -Logfile \"$AZ_BATCH_TASK_DIR/batch_script_log.txt\" -S batch_task bash -c \"trap 'echo \\$? > $AZ_BATCH_TASK_DIR/exit_code.txt' EXIT; batch_script_task $AZ_BATCH_TASK_DIR\"\n";
            batchNodeScript += "EXIT_CODE=$(cat \"$AZ_BATCH_TASK_DIR/exit_code.txt\")\n";
            batchNodeScript += "echo -e \"\\n\\nExit code: $EXIT_CODE\" >> \"$AZ_BATCH_TASK_DIR/batch_script_log.txt\"\n";
            batchNodeScript += "export AZCOPY_AUTO_LOGIN_TYPE=MSI\n";
            batchNodeScript += "azcopy copy \"$AZ_BATCH_TASK_DIR/batch_script_log.txt\" \"$LOG_URL\"\n";
            batchNodeScript += "LOG_ENV_URL=${LOG_URL/%batch_script_log.txt/batch_script_env.sh}\n";
            batchNodeScript += "azcopy copy \"$AZ_BATCH_TASK_DIR/batch_script_env.sh\" \"$LOG_ENV_URL\"\n";
            batchNodeScript += "if [ $EXIT_CODE -ne 0 ]; then\n";
            batchNodeScript += "    exit $EXIT_CODE\n";
            batchNodeScript += "fi\n";
            batchNodeScript += "echo Task complete\n";
            */

            batchNodeScript = $@"
set_defaults() {{
    if [ -z ""$AZ_BATCH_TASK_WORKING_DIR"" ]; then
        echo ""AZ_BATCH_TASK_WORKING_DIR is not set. Using default value of /mnt/cromwell/wd""
        export AZ_BATCH_TASK_WORKING_DIR=/mnt/cromwell/wd
    fi
    if [ -z ""$AZ_BATCH_TASK_DIR"" ]; then
        echo ""AZ_BATCH_TASK_DIR is not set. Using default value of /mnt/cromwell""
        export AZ_BATCH_TASK_DIR=/mnt/cromwell
    fi
}}
batch_script_task(){{
    local task_dir=$1
    source ""$task_dir/batch_script_env.sh"" || true
    set_defaults
{batchNodeScript}
}}
{pythonCommand}
export -f batch_script_task
export -f set_defaults
set_defaults
rm -f ""$AZ_BATCH_TASK_DIR/batch_script_log.txt""
export -p > ""$AZ_BATCH_TASK_DIR/batch_script_env.sh""
set >> ""$AZ_BATCH_TASK_DIR/batch_script_env.sh""
# trap 'echo \\$? > ""$AZ_BATCH_TASK_DIR/exit_code.txt""' EXIT; batch_script_task ""$AZ_BATCH_TASK_DIR""
screen -L -Logfile ""$AZ_BATCH_TASK_DIR/batch_script_log.txt"" -S batch_task bash -c ""trap 'echo \\$? > $AZ_BATCH_TASK_DIR/exit_code.txt' EXIT; batch_script_task $AZ_BATCH_TASK_DIR""
EXIT_CODE=$(cat ""$AZ_BATCH_TASK_DIR/exit_code.txt"")
echo -e ""\\n\\nExit code: $EXIT_CODE"" >> ""$AZ_BATCH_TASK_DIR/batch_script_log.txt""
OUTPUT_URL=$(get_log_destination_from_runner_url)
LOG_URL=""${{OUTPUT_URL}}batch_script_log.txt""
LOG_ENV_URL=""${{OUTPUT_URL}}batch_script_env.sh""
echo ""$LOG_URL""
export AZCOPY_AUTO_LOGIN_TYPE=MSI
azcopy copy ""$AZ_BATCH_TASK_DIR/batch_script_log.txt"" ""$LOG_URL""
azcopy copy ""$AZ_BATCH_TASK_DIR/batch_script_env.sh"" ""$LOG_ENV_URL""
if [ ""$EXIT_CODE"" -ne 0 ]; then
    exit ""$EXIT_CODE""
fi
echo Task complete
exit 0
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
