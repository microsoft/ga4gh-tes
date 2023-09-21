// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
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
    /// 
    /// </summary>
    public class TaskExecutionScriptingManager
    {
        private const string NodeTaskFilename = "TesTask.json";
        private const string NodeTaskRunnerFilename = "tRunner";
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
        /// <param name="additionalInputs"></param>
        /// <param name="installWgetIfRunningOnAlpine"></param>
        /// <param name="containerCleanupOptions"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<ScriptingAssetsInfo> PrepareScriptingAssetsAsync(TesTask tesTask, List<TesInput> additionalInputs,
            bool installWgetIfRunningOnAlpine, RuntimeContainerCleanupOptions containerCleanupOptions, CancellationToken cancellationToken)
        {
            try
            {
                var nodeTaskUrl = await CreateAndUploadNodeTaskAsync(tesTask, additionalInputs, containerCleanupOptions, cancellationToken);

                var batchScriptUrl = await CreateAndUploadBatchScriptAsync(tesTask, nodeTaskUrl, installWgetIfRunningOnAlpine, cancellationToken);

                return new ScriptingAssetsInfo(batchScriptUrl, nodeTaskUrl, BatchScriptFileName);
            }
            catch (Exception e)
            {
                logger.LogError(e,
                    "Failed to perform the preparation steps for the execution of the task on the Batch node");
                throw;
            }
        }

        /// <summary>
        /// Creates the run command the Batch node must run to execute the task
        /// </summary>
        /// <param name="scriptingAssets"></param>
        /// <returns></returns>
        public string ParseBatchRunCommand(ScriptingAssetsInfo scriptingAssets)
        {
            var batchRunCommand = $"/bin/bash -c {BatchNodeScriptBuilder.CreateWgetDownloadCommand(scriptingAssets.BatchScriptUrl, $"{BatchNodeScriptBuilder.BatchTaskDirEnvVarName}/{scriptingAssets.BatchScriptFileName}", setExecutable: true)} && {BatchNodeScriptBuilder.BatchTaskDirEnvVarName}/{scriptingAssets.BatchScriptFileName}";

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

        private async Task<string> CreateAndUploadBatchScriptAsync(TesTask tesTask, string nodeTaskUrl, bool installWgetIfRunningOnAlpine, CancellationToken cancellationToken)
        {
            logger.LogInformation($"Creating and uploading Batch script for Task ID: {tesTask.Id}");

            if (installWgetIfRunningOnAlpine)
            {
                batchNodeScriptBuilder.WithAlpineWgetInstallation();
            }

            var nodeTaskRunnerUrl = await storageAccessProvider.GetInternalTesBlobUrlAsync(NodeTaskRunnerFilename, cancellationToken);

            var batchNodeScript = batchNodeScriptBuilder.WithMetrics()
                .WithRunnerFilesDownloadUsingWget(nodeTaskRunnerUrl, nodeTaskUrl)
                .WithExecuteRunner()
                .WithLocalRuntimeSystemInformation()
                .Build();

            var batchNodeScriptUrl = await storageAccessProvider.GetInternalTesTaskBlobUrlAsync(tesTask, BatchScriptFileName, cancellationToken);

            await storageAccessProvider.UploadBlobAsync(new Uri(batchNodeScriptUrl), batchNodeScript, cancellationToken);

            logger.LogInformation($"Successfully created and uploaded Batch script for Task ID: {tesTask.Id}");

            return batchNodeScriptUrl;
        }

        private async Task<string> CreateAndUploadNodeTaskAsync(TesTask tesTask, List<TesInput> additionalInputs, RuntimeContainerCleanupOptions containerCleanupOptions, CancellationToken cancellationToken)
        {
            logger.LogInformation($"Creating and uploading node task definition file for Task ID: {tesTask.Id}");

            var nodeTask = await taskToNodeConverter.ToNodeTaskAsync(tesTask, additionalInputs, containerCleanupOptions, cancellationToken);

            var nodeTaskContent = JsonConvert.SerializeObject(nodeTask,
                new JsonSerializerSettings
                {
                    NullValueHandling = NullValueHandling.Ignore,
                    DefaultValueHandling = DefaultValueHandling.Ignore
                });

            var nodeTaskUrl = await UploadContentAsBlobToInternalTesLocationAsync(tesTask, nodeTaskContent, cancellationToken);

            logger.LogInformation($"Successfully created and uploaded node task definition file for Task ID: {tesTask.Id}");

            return nodeTaskUrl;
        }

        private async Task<string> UploadContentAsBlobToInternalTesLocationAsync(TesTask tesTask,
            string nodeTaskContent, CancellationToken cancellationToken)
        {
            var nodeTaskUrl =
                await storageAccessProvider.GetInternalTesTaskBlobUrlAsync(tesTask, NodeTaskFilename, cancellationToken);

            await storageAccessProvider.UploadBlobAsync(new Uri(nodeTaskUrl), nodeTaskContent, cancellationToken);
            return nodeTaskUrl;
        }
    }

    /// <summary>
    /// Contains information of the scripting assets required for the execution of a TES task in a Batch node using the TES runner.
    /// </summary>
    /// <param name="BatchScriptUrl"></param>
    /// <param name="NodeTaskUrl"></param>
    /// <param name="BatchScriptFileName"></param>
    public record ScriptingAssetsInfo(string BatchScriptUrl, string NodeTaskUrl, string BatchScriptFileName);
}
