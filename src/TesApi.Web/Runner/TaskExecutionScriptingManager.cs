﻿// Copyright (c) Microsoft Corporation.
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
        private const string BatchScriptFileName = "batch_script";

        private readonly IStorageAccessProvider storageAccessProvider;
        private readonly TaskToNodeTaskConverter taskToNodeConverter;
        private readonly ILogger<TaskExecutionScriptingManager> logger;

        /// <summary>
        /// Constructor of TaskExecutionScriptingManager
        /// </summary>
        /// <param name="storageAccessProvider"></param>
        /// <param name="taskToNodeConverter"></param>
        /// <param name="logger"></param>
        public TaskExecutionScriptingManager(IStorageAccessProvider storageAccessProvider, TaskToNodeTaskConverter taskToNodeConverter, ILogger<TaskExecutionScriptingManager> logger)
        {
            ArgumentNullException.ThrowIfNull(storageAccessProvider);
            ArgumentNullException.ThrowIfNull(taskToNodeConverter);
            ArgumentNullException.ThrowIfNull(logger);

            this.storageAccessProvider = storageAccessProvider;
            this.taskToNodeConverter = taskToNodeConverter;
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
                        DefaultValueHandling = DefaultValueHandling.Ignore,
                        Formatting = Formatting.Indented,
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

            var batchNodeScript = new BatchNodeScriptBuilder()
                .WithMetrics()
                .WithRunnerTaskDownloadUsingWget(nodeTaskUrl)
                .WithExecuteRunner()
                .WithLocalRuntimeSystemInformation()
                .Build();

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
