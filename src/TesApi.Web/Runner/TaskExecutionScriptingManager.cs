// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Tes.Models;
using Tes.Runner.Models;
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

        private static readonly JsonSerializerSettings IndentedSerializerSettings = new()
        {
            NullValueHandling = NullValueHandling.Ignore,
            DefaultValueHandling = DefaultValueHandling.Ignore,
            Formatting = Formatting.Indented,
        };

        private static readonly JsonSerializerSettings DefaultSerializerSettings = new()
        {
            NullValueHandling = NullValueHandling.Ignore,
            DefaultValueHandling = DefaultValueHandling.Ignore
        };

        private readonly IStorageAccessProvider storageAccessProvider;
        private readonly TaskToNodeTaskConverter taskToNodeConverter;
        private readonly ILogger<TaskExecutionScriptingManager> logger;
        private readonly bool advancedVmPerformanceMonitoring;

        /// <summary>
        /// Constructor of TaskExecutionScriptingManager
        /// </summary>
        /// <param name="storageAccessProvider"></param>
        /// <param name="taskToNodeConverter"></param>
        /// <param name="batchNodesOptions"></param>
        /// <param name="logger"></param>
        public TaskExecutionScriptingManager(IStorageAccessProvider storageAccessProvider, TaskToNodeTaskConverter taskToNodeConverter, IOptions<Options.BatchNodesOptions> batchNodesOptions, ILogger<TaskExecutionScriptingManager> logger)
        {
            ArgumentNullException.ThrowIfNull(storageAccessProvider);
            ArgumentNullException.ThrowIfNull(taskToNodeConverter);
            ArgumentNullException.ThrowIfNull(logger);

            this.storageAccessProvider = storageAccessProvider;
            this.taskToNodeConverter = taskToNodeConverter;
            this.advancedVmPerformanceMonitoring = batchNodesOptions.Value.AdvancedVmPerformanceMonitoringEnabled;
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

                List<KeyValuePair<string, string>> environment =
                    [new(nameof(NodeTaskResolverOptions), JsonConvert.SerializeObject(
                        taskToNodeConverter.ToNodeTaskResolverOptions(tesTask, nodeTaskConversionOptions),
                        DefaultSerializerSettings))];

                return new BatchScriptAssetsInfo(nodeTaskUrl, environment.ToDictionary().AsReadOnly());
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
                var severTesTaskContent = JsonConvert.SerializeObject(tesTask, IndentedSerializerSettings);

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
            var batchRunCommand = $"/usr/bin/env -S \"{BatchScheduler.BatchNodeSharedEnvVar}/{BatchScheduler.NodeTaskRunnerFilename} -i '{(new Azure.Storage.Blobs.BlobUriBuilder(batchScriptAssets.NodeTaskUrl) { Sas = null }).ToUri().AbsoluteUri}'\"";

            logger.LogInformation("Run command: {RunCommand}", batchRunCommand);

            return batchRunCommand;
        }

        private async Task<Uri> CreateAndUploadNodeTaskAsync(TesTask tesTask, NodeTaskConversionOptions nodeTaskConversionOptions, CancellationToken cancellationToken)
        {
            logger.LogInformation("Creating and uploading node task definition file for Task ID: {TesTask}", tesTask.Id);

            var nodeTask = await taskToNodeConverter.ToNodeTaskAsync(tesTask, nodeTaskConversionOptions, cancellationToken);

            var nodeTaskContent = JsonConvert.SerializeObject(nodeTask, DefaultSerializerSettings);

            var nodeTaskUrl = await UploadContentAsBlobToInternalTesLocationAsync(tesTask, nodeTaskContent, NodeTaskFilename, cancellationToken);

            logger.LogInformation("Successfully created and uploaded node task definition file for Task ID: {TesTask}", tesTask.Id);

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
    /// <param name="NodeTaskUrl"></param>
    /// <param name="Environment"></param>
    public record BatchScriptAssetsInfo(Uri NodeTaskUrl, IReadOnlyDictionary<string, string> Environment);
}
