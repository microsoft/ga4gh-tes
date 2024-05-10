// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.


using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Tes.Extensions;
using Tes.Models;
using TesApi.Web.Extensions;
using TesApi.Web.Management;
using TesApi.Web.Management.Models.Quotas;
using TesApi.Web.Options;
using TesApi.Web.Runner;
using TesApi.Web.Storage;
using BatchModels = Microsoft.Azure.Management.Batch.Models;
using TesException = Tes.Models.TesException;
using TesFileType = Tes.Models.TesFileType;
using TesInput = Tes.Models.TesInput;
using TesResources = Tes.Models.TesResources;
using TesState = Tes.Models.TesState;
using TesTask = Tes.Models.TesTask;
using VirtualMachineInformation = Tes.Models.VirtualMachineInformation;

namespace TesApi.Web
{
    /// <summary>
    /// Orchestrates <see cref="Tes.Models.TesTask"/>s on Azure Batch
    /// </summary>
    public partial class BatchScheduler : IBatchScheduler
    {
        internal const string PoolDeprecated = "CoA-TES-HostName";
        internal const string PoolMetadata = "CoA-TES-Metadata";

        /// <summary>
        /// Name of <c>$</c> prefixed environment variable to place resources shared by all tasks on each compute node in a pool.
        /// </summary>
        public const string BatchNodeSharedEnvVar = "$AZ_BATCH_NODE_SHARED_DIR";

        /// <summary>
        /// Name of <c>$</c> prefixed environment variable of the working directory of the running task.
        /// </summary>
        public const string BatchNodeTaskWorkingDirEnvVar = "$AZ_BATCH_TASK_WORKING_DIR";

        private const string AzureSupportUrl = "https://portal.azure.com/#blade/Microsoft_Azure_Support/HelpAndSupportBlade/newsupportrequest";
        private const int PoolKeyLength = 55; // 64 max pool name length - 9 chars generating unique pool names
        private const int DefaultCoreCount = 1;
        private const int DefaultMemoryGb = 2;
        private const int DefaultDiskGb = 10;
        private const string CromwellScriptFileName = "script";
        private const string StartTaskScriptFilename = "start-task.sh";
        private const string NodeTaskRunnerFilename = "tes-runner";
        private const string NodeTaskRunnerMD5HashFilename = NodeTaskRunnerFilename + ".md5";
        private readonly ILogger logger;
        private readonly IAzureProxy azureProxy;
        private readonly IStorageAccessProvider storageAccessProvider;
        private readonly IBatchQuotaVerifier quotaVerifier;
        private readonly IBatchSkuInformationProvider skuInformationProvider;
        private readonly List<TesTaskStateTransition> tesTaskStateTransitions;
        private readonly bool usePreemptibleVmsOnly;
        private readonly string batchNodesSubnetId;
        private readonly bool disableBatchNodesPublicIpAddress;
        private readonly TimeSpan poolLifetime;
        private readonly BatchNodeInfo gen2BatchNodeInfo;
        private readonly BatchNodeInfo gen1BatchNodeInfo;
        private readonly string defaultStorageAccountName;
        private readonly string globalStartTaskPath;
        private readonly string globalManagedIdentity;
        private readonly string batchPrefix;
        private readonly IBatchPoolFactory _batchPoolFactory;
        private readonly IAllowedVmSizesService allowedVmSizesService;
        private readonly TaskExecutionScriptingManager taskExecutionScriptingManager;
        private readonly string runnerMD5;
        private readonly string drsHubApiHost;

        private HashSet<string> onlyLogBatchTaskStateOnce = [];

        /// <summary>
        /// Orchestrates <see cref="Tes.Models.TesTask"/>s on Azure Batch
        /// </summary>
        /// <param name="logger">Logger <see cref="ILogger"/></param>
        /// <param name="batchGen1Options">Configuration of <see cref="BatchImageGeneration1Options"/></param>
        /// <param name="batchGen2Options">Configuration of <see cref="BatchImageGeneration2Options"/></param>
        /// <param name="drsHubOptions">Configuration of <see cref="DrsHubOptions"/></param>
        /// <param name="storageOptions">Configuration of <see cref="StorageOptions"/></param>
        /// <param name="batchNodesOptions">Configuration of <see cref="BatchNodesOptions"/></param>
        /// <param name="batchSchedulingOptions">Configuration of <see cref="BatchSchedulingOptions"/></param>
        /// <param name="azureProxy">Azure proxy <see cref="IAzureProxy"/></param>
        /// <param name="storageAccessProvider">Storage access provider <see cref="IStorageAccessProvider"/></param>
        /// <param name="quotaVerifier">Quota verifier <see cref="IBatchQuotaVerifier"/>></param>
        /// <param name="skuInformationProvider">Sku information provider <see cref="IBatchSkuInformationProvider"/></param>
        /// <param name="poolFactory">Batch pool factory <see cref="IBatchPoolFactory"/></param>
        /// <param name="allowedVmSizesService">Service to get allowed vm sizes.</param>
        /// <param name="taskExecutionScriptingManager"><see cref="taskExecutionScriptingManager"/></param>
        public BatchScheduler(
            ILogger<BatchScheduler> logger,
            IOptions<Options.BatchImageGeneration1Options> batchGen1Options,
            IOptions<Options.BatchImageGeneration2Options> batchGen2Options,
            IOptions<Options.DrsHubOptions> drsHubOptions,
            IOptions<Options.StorageOptions> storageOptions,
            IOptions<Options.BatchNodesOptions> batchNodesOptions,
            IOptions<Options.BatchSchedulingOptions> batchSchedulingOptions,
            IAzureProxy azureProxy,
            IStorageAccessProvider storageAccessProvider,
            IBatchQuotaVerifier quotaVerifier,
            IBatchSkuInformationProvider skuInformationProvider,
            IBatchPoolFactory poolFactory,
            IAllowedVmSizesService allowedVmSizesService,
            TaskExecutionScriptingManager taskExecutionScriptingManager)
        {
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(azureProxy);
            ArgumentNullException.ThrowIfNull(storageAccessProvider);
            ArgumentNullException.ThrowIfNull(quotaVerifier);
            ArgumentNullException.ThrowIfNull(skuInformationProvider);
            ArgumentNullException.ThrowIfNull(poolFactory);
            ArgumentNullException.ThrowIfNull(taskExecutionScriptingManager);

            this.logger = logger;
            this.azureProxy = azureProxy;
            this.storageAccessProvider = storageAccessProvider;
            this.quotaVerifier = quotaVerifier;
            this.skuInformationProvider = skuInformationProvider;

            this.usePreemptibleVmsOnly = batchSchedulingOptions.Value.UsePreemptibleVmsOnly;
            this.batchNodesSubnetId = batchNodesOptions.Value.SubnetId;
            this.disableBatchNodesPublicIpAddress = batchNodesOptions.Value.DisablePublicIpAddress;
            this.poolLifetime = TimeSpan.FromDays(batchSchedulingOptions.Value.PoolRotationForcedDays == 0 ? Options.BatchSchedulingOptions.DefaultPoolRotationForcedDays : batchSchedulingOptions.Value.PoolRotationForcedDays);
            this.defaultStorageAccountName = storageOptions.Value.DefaultAccountName;
            this.globalStartTaskPath = StandardizeStartTaskPath(batchNodesOptions.Value.GlobalStartTask, this.defaultStorageAccountName);
            this.globalManagedIdentity = batchNodesOptions.Value.GlobalManagedIdentity;
            this.allowedVmSizesService = allowedVmSizesService;
            this.taskExecutionScriptingManager = taskExecutionScriptingManager;
            _batchPoolFactory = poolFactory;
            batchPrefix = batchSchedulingOptions.Value.Prefix;
            logger.LogInformation("BatchPrefix: {BatchPrefix}", batchPrefix);
            this.runnerMD5 = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, $"scripts/{NodeTaskRunnerMD5HashFilename}")).Trim();

            this.gen2BatchNodeInfo = new BatchNodeInfo
            {
                BatchImageOffer = batchGen2Options.Value.Offer,
                BatchImagePublisher = batchGen2Options.Value.Publisher,
                BatchImageSku = batchGen2Options.Value.Sku,
                BatchImageVersion = batchGen2Options.Value.Version,
                BatchNodeAgentSkuId = batchGen2Options.Value.NodeAgentSkuId
            };

            this.gen1BatchNodeInfo = new BatchNodeInfo
            {
                BatchImageOffer = batchGen1Options.Value.Offer,
                BatchImagePublisher = batchGen1Options.Value.Publisher,
                BatchImageSku = batchGen1Options.Value.Sku,
                BatchImageVersion = batchGen1Options.Value.Version,
                BatchNodeAgentSkuId = batchGen1Options.Value.NodeAgentSkuId
            };
            drsHubApiHost = drsHubOptions.Value?.Url;
            logger.LogInformation("usePreemptibleVmsOnly: {UsePreemptibleVmsOnly}", usePreemptibleVmsOnly);

            static bool tesTaskIsQueuedInitializingOrRunning(TesTask tesTask) => tesTask.State == TesState.QUEUED || tesTask.State == TesState.INITIALIZING || tesTask.State == TesState.RUNNING;
            static bool tesTaskIsInitializingOrRunning(TesTask tesTask) => tesTask.State == TesState.INITIALIZING || tesTask.State == TesState.RUNNING;
            static bool tesTaskIsQueuedOrInitializing(TesTask tesTask) => tesTask.State == TesState.QUEUED || tesTask.State == TesState.INITIALIZING;
            static bool tesTaskIsQueued(TesTask tesTask) => tesTask.State == TesState.QUEUED;
            static bool tesTaskCancellationRequested(TesTask tesTask) => tesTask.State == TesState.CANCELED && tesTask.IsCancelRequested;

            static void SetTaskStateAndLog(TesTask tesTask, TesState newTaskState, CombinedBatchTaskInfo batchInfo)
            {
                tesTask.State = newTaskState;

                var tesTaskLog = tesTask.GetOrAddTesTaskLog();
                var tesTaskExecutorLog = tesTaskLog.GetOrAddExecutorLog();

                tesTaskLog.BatchNodeMetrics = batchInfo.BatchNodeMetrics;
                tesTaskLog.CromwellResultCode = batchInfo.CromwellRcCode;
                tesTaskLog.EndTime = DateTime.UtcNow;
                tesTaskExecutorLog.StartTime = batchInfo.BatchTaskStartTime;
                tesTaskExecutorLog.EndTime = batchInfo.BatchTaskEndTime;
                tesTaskExecutorLog.ExitCode = batchInfo.BatchTaskExitCode;

                // Only accurate when the task completes successfully, otherwise it's the Batch time as reported from Batch
                // TODO this could get large; why?
                //var timefromCoAScriptCompletionToBatchTaskDetectedComplete = tesTaskLog.EndTime - tesTaskExecutorLog.EndTime;

                tesTask.SetFailureReason(batchInfo.FailureReason);

                if (batchInfo.SystemLogItems is not null)
                {
                    tesTask.AddToSystemLog(batchInfo.SystemLogItems);
                }
                else if (!string.IsNullOrWhiteSpace(batchInfo.AlternateSystemLogItem))
                {
                    tesTask.AddToSystemLog([batchInfo.AlternateSystemLogItem]);
                }
            }

            async Task SetTaskCompleted(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
            {
                SetTaskStateAndLog(tesTask, TesState.COMPLETE, batchInfo);
                await DeleteBatchTaskAsync(azureProxy, tesTask, batchInfo, cancellationToken);
            }

            async Task SetTaskExecutorError(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
            {
                await AddProcessLogsIfAvailable(tesTask, cancellationToken);
                SetTaskStateAndLog(tesTask, TesState.EXECUTOR_ERROR, batchInfo);
                await DeleteBatchTaskAsync(azureProxy, tesTask, batchInfo, cancellationToken);
            }

            async Task DeleteBatchTaskAndSetTaskStateAsync(TesTask tesTask, TesState newTaskState, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
            {
                SetTaskStateAndLog(tesTask, newTaskState, batchInfo);
                await DeleteBatchTaskAsync(tesTask, batchInfo.Pool, cancellationToken);
            }

            Task DeleteBatchTaskAndSetTaskExecutorErrorAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken) => DeleteBatchTaskAndSetTaskStateAsync(tesTask, TesState.EXECUTOR_ERROR, batchInfo, cancellationToken);
            Task DeleteBatchTaskAndSetTaskSystemErrorAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken) => DeleteBatchTaskAndSetTaskStateAsync(tesTask, TesState.SYSTEM_ERROR, batchInfo, cancellationToken);

            Task DeleteBatchTaskAndRequeueTaskAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
                => ++tesTask.ErrorCount > 3
                    ? AddSystemLogAndDeleteBatchTaskAndSetTaskExecutorErrorAsync(tesTask, batchInfo, "System Error: Retry count exceeded.", cancellationToken)
                    : DeleteBatchTaskAndSetTaskStateAsync(tesTask, TesState.QUEUED, batchInfo, cancellationToken);

            Task AddSystemLogAndDeleteBatchTaskAndSetTaskExecutorErrorAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo, string alternateSystemLogItem, CancellationToken cancellationToken)
            {
                batchInfo.SystemLogItems ??= Enumerable.Empty<string>().Append(alternateSystemLogItem);
                return DeleteBatchTaskAndSetTaskExecutorErrorAsync(tesTask, batchInfo, cancellationToken);
            }

            async Task CancelTaskAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
            {
                await DeleteBatchTaskAsync(tesTask, batchInfo.Pool, cancellationToken);
                tesTask.IsCancelRequested = false;
            }

            Task HandlePreemptedNodeAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
            {
                logger.LogInformation("The TesTask {TesTask}'s node was preempted. It will be automatically rescheduled.", tesTask.Id);
                SetTaskStateAndLog(tesTask, TesState.INITIALIZING, batchInfo);
                return Task.CompletedTask;
            }

            tesTaskStateTransitions =
            [
                new(tesTaskCancellationRequested, batchTaskState: null, alternateSystemLogItem: null, CancelTaskAsync),
                new(tesTaskIsQueued, BatchTaskState.JobNotFound, alternateSystemLogItem: null, (tesTask, _, ct) => AddBatchTaskAsync(tesTask, ct)),
                new(tesTaskIsQueued, BatchTaskState.MissingBatchTask, alternateSystemLogItem: null, (tesTask, batchInfo, ct) => AddBatchTaskAsync(tesTask, ct)),
                new(tesTaskIsQueued, BatchTaskState.Initializing, alternateSystemLogItem: null, (tesTask, _) => tesTask.State = TesState.INITIALIZING),
                new(tesTaskIsQueuedOrInitializing, BatchTaskState.NodeAllocationFailed, alternateSystemLogItem: null, DeleteBatchTaskAndRequeueTaskAsync),
                new(tesTaskIsQueuedOrInitializing, BatchTaskState.Running, alternateSystemLogItem: null, (tesTask, _) => tesTask.State = TesState.RUNNING),
                new(tesTaskIsQueuedInitializingOrRunning, BatchTaskState.MoreThanOneActiveJobOrTaskFound, BatchTaskState.MoreThanOneActiveJobOrTaskFound.ToString(), DeleteBatchTaskAndSetTaskSystemErrorAsync),
                new(tesTaskIsQueuedInitializingOrRunning, BatchTaskState.CompletedSuccessfully, alternateSystemLogItem: null, SetTaskCompleted),
                new(tesTaskIsQueuedInitializingOrRunning, BatchTaskState.CompletedWithErrors, "Please open an issue. There should have been an error reported here.", SetTaskExecutorError),
                new(tesTaskIsQueuedInitializingOrRunning, BatchTaskState.ActiveJobWithMissingAutoPool, alternateSystemLogItem: null, DeleteBatchTaskAndRequeueTaskAsync),
                new(tesTaskIsQueuedInitializingOrRunning, BatchTaskState.NodeFailedDuringStartupOrExecution, "Please open an issue. There should have been an error reported here.", DeleteBatchTaskAndSetTaskExecutorErrorAsync),
                new(tesTaskIsQueuedInitializingOrRunning, BatchTaskState.NodeUnusable, "Please open an issue. There should have been an error reported here.", DeleteBatchTaskAndSetTaskExecutorErrorAsync),
                new(tesTaskIsInitializingOrRunning, BatchTaskState.JobNotFound, BatchTaskState.JobNotFound.ToString(), DeleteBatchTaskAndRequeueTaskAsync),
                new(tesTaskIsInitializingOrRunning, BatchTaskState.MissingBatchTask, BatchTaskState.MissingBatchTask.ToString(), DeleteBatchTaskAndSetTaskSystemErrorAsync),
                new(tesTaskIsInitializingOrRunning, BatchTaskState.NodePreempted, alternateSystemLogItem: null, HandlePreemptedNodeAsync)
            ];
        }

        private async Task AddProcessLogsIfAvailable(TesTask tesTask, CancellationToken cancellationToken)
        {
            try
            {
                var directoryUri = await storageAccessProvider.GetInternalTesTaskBlobUrlAsync(tesTask, string.Empty, cancellationToken);

                // Process log naming convention is (mostly) established in Tes.Runner.Logs.AppendBlobLogPublisher, specifically these methods:
                // https://github.com/microsoft/ga4gh-tes/blob/6e120d33f78c7a36cffe953c74b55cba7cfbf7fc/src/Tes.Runner/Logs/AppendBlobLogPublisher.cs#L28
                // https://github.com/microsoft/ga4gh-tes/blob/6e120d33f78c7a36cffe953c74b55cba7cfbf7fc/src/Tes.Runner/Logs/AppendBlobLogPublisher.cs#L39

                // Get any logs the task runner left. Look for the latest set in this order: upload, exec, download
                foreach (var prefix in new[] { "upload_std", "exec_std", "download_std" })
                {
                    var logs = FilterByPrefix(prefix, await azureProxy.ListBlobsAsync(directoryUri, cancellationToken));

                    if (logs.Any())
                    {
                        if (prefix.StartsWith("exec_"))
                        {
                            var log = tesTask.GetOrAddTesTaskLog().GetOrAddExecutorLog();

                            foreach (var (type, action) in new (string, Action<string>)[] { ("stderr", list => log.Stderr = list), ("stdout", list => log.Stdout = list) })
                            {
                                var list = logs.Where(blob => type.Equals(blob.BlobNameParts[1], StringComparison.OrdinalIgnoreCase)).ToList();

                                if (list.Any())
                                {
                                    action(JsonArray(list.Select(blob => blob.BlobUri.AbsoluteUri)));
                                }
                            }
                        }

                        tesTask.AddToSystemLog(Enumerable.Empty<string>()
                            .Append("Possibly relevant logs:")
                            .Concat(logs.Select(log => log.BlobUri.AbsoluteUri)));

                        return;
                    }
                }

#pragma warning disable IDE0305 // Simplify collection initialization
                static IList<(Uri BlobUri, string[] BlobNameParts)> FilterByPrefix(string blobNameStartsWith, IEnumerable<Microsoft.WindowsAzure.Storage.Blob.CloudBlob> blobs)
                    => blobs.Select(blob => (BlobUri: new Azure.Storage.Blobs.BlobUriBuilder(blob.Uri) { Sas = null }.ToUri(), BlobName: blob.Name.Split('/').Last()))
                        .Where(blob => blob.BlobName.EndsWith(".txt") && blob.BlobName.StartsWith(blobNameStartsWith))
                        .Select(blob => (blob.BlobUri, BlobNameParts: blob.BlobName.Split('_', 4)))
                        .OrderBy(blob => string.Join('_', blob.BlobNameParts.Take(3)))
                        .ThenBy(blob => blob.BlobNameParts.Length < 3 ? -1 : int.Parse(blob.BlobNameParts[3][..blob.BlobNameParts[3].IndexOf('.')], System.Globalization.CultureInfo.InvariantCulture))
                        .ToList();
#pragma warning restore IDE0305 // Simplify collection initialization

                static string JsonArray(IEnumerable<string> items)
                    => System.Text.Json.JsonSerializer.Serialize(items.ToArray(), new System.Text.Json.JsonSerializerOptions(System.Text.Json.JsonSerializerOptions.Default) { WriteIndented = true });
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to find and append process logs on task {TesTask}", tesTask.Id);
            }
        }

        private Task DeleteBatchTaskAsync(TesTask tesTask, string poolId, CancellationToken cancellationToken)
            => azureProxy.DeleteBatchTaskAsync(tesTask.Id, poolId, cancellationToken);

        private async Task DeleteBatchTaskAsync(IAzureProxy azureProxy, TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
        {
            var batchDeletionExceptions = new List<Exception>();

            try
            {
                await DeleteBatchTaskAsync(tesTask, batchInfo.Pool, cancellationToken);
            }
            catch (Exception exc)
            {
                logger.LogError(exc, $"Exception deleting batch task with tesTask.Id: {tesTask?.Id}");
                batchDeletionExceptions.Add(exc);
            }

            if (batchDeletionExceptions.Any())
            {
                throw new AggregateException(batchDeletionExceptions);
            }
        }

        /// <summary>
        /// Creates a wget command to robustly download a file
        /// </summary>
        /// <param name="urlToDownload">URL to download</param>
        /// <param name="localFilePathDownloadLocation">Filename for the output file</param>
        /// <param name="setExecutable">Whether the file should be made executable or not</param>
        /// <returns>The command to execute</returns>
        public static string CreateWgetDownloadCommand(Uri urlToDownload, string localFilePathDownloadLocation, bool setExecutable = false)
        {
            ArgumentNullException.ThrowIfNull(urlToDownload);
            ArgumentException.ThrowIfNullOrWhiteSpace(localFilePathDownloadLocation);

            var command = $"wget --no-verbose --https-only --timeout=20 --waitretry=1 --tries=9 --retry-connrefused --continue -O {localFilePathDownloadLocation} '{urlToDownload.AbsoluteUri}' && sleep 5s";

            if (setExecutable)
            {
                command += $" && chmod +x {localFilePathDownloadLocation}";
            }

            return command;
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudPool> GetCloudPools(CancellationToken cancellationToken)
            => azureProxy.GetActivePoolsAsync(batchPrefix);

        /// <inheritdoc/>
        public async Task LoadExistingPoolsAsync(CancellationToken cancellationToken)
        {
            await foreach (var cloudPool in GetCloudPools(cancellationToken).WithCancellation(cancellationToken))
            {
                try
                {
                    var batchPool = _batchPoolFactory.CreateNew();
                    await batchPool.AssignPoolAsync(cloudPool, cancellationToken);
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, "When retrieving previously created batch pools and jobs, there were one or more failures when trying to access batch pool {PoolId} and/or its associated job.", cloudPool.Id);
                }
            }
        }

        /// <inheritdoc/>
        public async Task UploadTaskRunnerIfNeeded(CancellationToken cancellationToken)
        {
            var blobUri = await storageAccessProvider.GetInternalTesBlobUrlAsync(NodeTaskRunnerFilename, cancellationToken);
            var blobProperties = await azureProxy.GetBlobPropertiesAsync(blobUri, cancellationToken);
            if (!runnerMD5.Equals(blobProperties?.ContentMD5, StringComparison.OrdinalIgnoreCase))
            {
                await azureProxy.UploadBlobFromFileAsync(blobUri, $"scripts/{NodeTaskRunnerFilename}", cancellationToken);
            }
        }

        /// <summary>
        /// Iteratively manages execution of a <see cref="TesTask"/> on Azure Batch until completion or failure
        /// </summary>
        /// <param name="tesTask">The <see cref="TesTask"/></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>True if the TES task needs to be persisted.</returns>
        public async ValueTask<bool> ProcessTesTaskAsync(TesTask tesTask, CancellationToken cancellationToken)
        {
            var combinedBatchTaskInfo = await GetBatchTaskStateAsync(tesTask, cancellationToken);
            const string template = "TES task: {TesTask} TES task state: {TesTaskState} BatchTaskState: {BatchTaskState}";
            var msg = string.Format(ConvertTemplateToFormat(template), tesTask.Id, tesTask.State.ToString(), combinedBatchTaskInfo.BatchTaskState.ToString());

            if (onlyLogBatchTaskStateOnce.Add(msg))
            {
                logger.LogInformation(template, tesTask.Id, tesTask.State.ToString(), combinedBatchTaskInfo.BatchTaskState.ToString());
            }

            return await HandleTesTaskTransitionAsync(tesTask, combinedBatchTaskInfo, cancellationToken);

            static string ConvertTemplateToFormat(string template)
                => string.Join(null, template.Split('{', '}').Select((s, i) => (s, i)).Select(t => t.i % 2 == 0 ? t.s : $"{{{t.i / 2}}}"));
        }

        /// <summary>
        /// Garbage collects the old batch task state log hashset
        /// </summary>
        public void ClearBatchLogState()
        {
            if (onlyLogBatchTaskStateOnce.Count > 0)
            {
                onlyLogBatchTaskStateOnce = new();
            }
        }

        /// <summary>
        /// Get the parent path of the given path
        /// </summary>
        /// <param name="path">The path</param>
        /// <returns>The parent path</returns>
        private static string GetParentPath(string path)
        {
            if (string.IsNullOrEmpty(path))
            {
                return null;
            }

            var pathComponents = path.TrimEnd('/').Split('/');

            return string.Join('/', pathComponents.Take(pathComponents.Length - 1));
        }

        private static string GetParentUrl(string url)
        {
            if (!Uri.TryCreate(url, UriKind.Absolute, out var uri) || Uri.CheckHostName(uri.Host) <= UriHostNameType.Basic)
            {
                return GetParentPath(url).TrimStart('/'); // Continue support of Cromwell in local filesystem configuration
            }

            var builder = new UriBuilder(url);
            builder.Path = GetParentPath(builder.Path);
            return builder.ToString();
        }

        private static string StandardizeStartTaskPath(string startTaskPath, string defaultStorageAccount)
        {
            if (string.IsNullOrWhiteSpace(startTaskPath) || startTaskPath.StartsWith($"/{defaultStorageAccount}"))
            {
                return startTaskPath;
            }
            else
            {
                return $"/{defaultStorageAccount}{startTaskPath}";
            }
        }

        /// <summary>
        /// Adds a new Azure Batch pool/job/task for the given <see cref="TesTask"/>
        /// </summary>
        /// <param name="tesTask">The <see cref="TesTask"/> to schedule on Azure Batch</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>A task to await</returns>
        private async Task AddBatchTaskAsync(TesTask tesTask, CancellationToken cancellationToken)
        {
            string poolId = null;
            string poolKey = null;

            try
            {
                var identities = new List<string>();

                if (!string.IsNullOrWhiteSpace(globalManagedIdentity))
                {
                    identities.Add(globalManagedIdentity);
                }

                if (tesTask.Resources?.ContainsBackendParameterValue(TesResources.SupportedBackendParameters.workflow_execution_identity) == true)
                {
                    identities.Add(tesTask.Resources?.GetBackendParameterValue(TesResources.SupportedBackendParameters.workflow_execution_identity));
                }

                var virtualMachineInfo = await GetVmSizeAsync(tesTask, cancellationToken);

                (poolKey, var displayName) = GetPoolKey(tesTask, virtualMachineInfo, identities, cancellationToken);
                await quotaVerifier.CheckBatchAccountQuotasAsync(virtualMachineInfo, needPoolOrJobQuotaCheck: !IsPoolAvailable(poolKey), cancellationToken: cancellationToken);

                var tesTaskLog = tesTask.AddTesTaskLog();
                tesTaskLog.VirtualMachineInfo = virtualMachineInfo;


                var useGen2 = virtualMachineInfo.HyperVGenerations?.Contains("V2", StringComparer.OrdinalIgnoreCase);
                poolId = (await GetOrAddPoolAsync(
                    key: poolKey,
                    isPreemptable: virtualMachineInfo.LowPriority,
                    modelPoolFactory: (id, ct) => GetPoolSpecification(
                        name: id,
                        displayName: displayName,
                        poolIdentity: GetBatchPoolIdentity(identities.ToArray()),
                        vmSize: virtualMachineInfo.VmSize,
                        vmFamily: virtualMachineInfo.VmFamily,
                        preemptable: virtualMachineInfo.LowPriority,
                        nodeInfo: useGen2.GetValueOrDefault() ? gen2BatchNodeInfo : gen1BatchNodeInfo,
                        encryptionAtHostSupported: virtualMachineInfo.EncryptionAtHostSupported,
                        cancellationToken: ct),
                    cancellationToken: cancellationToken)
                    ).PoolId;
                var jobOrTaskId = $"{tesTask.Id}-{tesTask.Logs.Count}";

                tesTask.PoolId = poolId;
                var cloudTask = await ConvertTesTaskToBatchTaskUsingRunnerAsync(jobOrTaskId, tesTask, cancellationToken);
                logger.LogInformation($"Creating batch task for TES task {tesTask.Id}. Using VM size {virtualMachineInfo.VmSize}.");
                await azureProxy.AddBatchTaskAsync(tesTask.Id, cloudTask, poolId, cancellationToken);

                tesTaskLog.StartTime = DateTimeOffset.UtcNow;
                tesTask.State = TesState.INITIALIZING;
                poolId = null;
            }
            catch (AggregateException aggregateException)
            {
                foreach (var exception in aggregateException.Flatten().InnerExceptions)
                {
                    HandleException(exception);
                }
            }
            catch (Exception exception)
            {
                HandleException(exception);
            }

            void HandleException(Exception exception)
            {
                switch (exception)
                {
                    case AzureBatchPoolCreationException azureBatchPoolCreationException:
                        if (!azureBatchPoolCreationException.IsTimeout && !azureBatchPoolCreationException.IsJobQuota && !azureBatchPoolCreationException.IsPoolQuota && azureBatchPoolCreationException.InnerException is not null)
                        {
                            HandleException(azureBatchPoolCreationException.InnerException);
                            return;
                        }

                        logger.LogWarning(azureBatchPoolCreationException, "TES task: {TesTask} AzureBatchPoolCreationException.Message: {ExceptionMessage}. This might be a transient issue. Task will remain with state QUEUED. Confirmed timeout: {ConfirmedTimeout}", tesTask.Id, azureBatchPoolCreationException.Message, azureBatchPoolCreationException.IsTimeout);

                        if (azureBatchPoolCreationException.IsJobQuota || azureBatchPoolCreationException.IsPoolQuota)
                        {
                            neededPools.Add(poolKey);
                            tesTask.SetWarning(azureBatchPoolCreationException.InnerException switch
                            {
                                null => "Unknown reason",
                                Microsoft.Rest.Azure.CloudException cloudException => cloudException.Body.Message,
                                var e when e is BatchException batchException && batchException.InnerException is Microsoft.Azure.Batch.Protocol.Models.BatchErrorException batchErrorException => batchErrorException.Body.Message.Value,
                                _ => "Unknown reason",
                            },
                                Array.Empty<string>());
                        }

                        break;

                    case AzureBatchQuotaMaxedOutException azureBatchQuotaMaxedOutException:
                        logger.LogWarning("TES task: {TesTask} AzureBatchQuotaMaxedOutException.Message: {ExceptionMessage}. Not enough quota available. Task will remain with state QUEUED.", tesTask.Id, azureBatchQuotaMaxedOutException.Message);
                        neededPools.Add(poolKey);
                        break;

                    case AzureBatchLowQuotaException azureBatchLowQuotaException:
                        tesTask.State = TesState.SYSTEM_ERROR;
                        tesTask.AddTesTaskLog(); // Adding new log here because this exception is thrown from CheckBatchAccountQuotas() and AddTesTaskLog() above is called after that. This way each attempt will have its own log entry.
                        tesTask.SetFailureReason("InsufficientBatchQuota", azureBatchLowQuotaException.Message);
                        logger.LogError(azureBatchLowQuotaException, "TES task: {TesTask} AzureBatchLowQuotaException.Message: {ExceptionMessage}", tesTask.Id, azureBatchLowQuotaException.Message);
                        break;

                    case AzureBatchVirtualMachineAvailabilityException azureBatchVirtualMachineAvailabilityException:
                        tesTask.State = TesState.SYSTEM_ERROR;
                        tesTask.AddTesTaskLog(); // Adding new log here because this exception is thrown from GetVmSizeAsync() and AddTesTaskLog() above is called after that. This way each attempt will have its own log entry.
                        tesTask.SetFailureReason("NoVmSizeAvailable", azureBatchVirtualMachineAvailabilityException.Message);
                        logger.LogError(azureBatchVirtualMachineAvailabilityException, "TES task: {TesTask} AzureBatchVirtualMachineAvailabilityException.Message: {ExceptionMessage}", tesTask.Id, azureBatchVirtualMachineAvailabilityException.Message);
                        break;

                    case TesException tesException:
                        tesTask.State = TesState.SYSTEM_ERROR;
                        tesTask.SetFailureReason(tesException);
                        logger.LogError(tesException, "TES task: {TesTask} TesException.Message: {ExceptionMessage}", tesTask.Id, tesException.Message);
                        break;

                    case BatchClientException batchClientException:
                        tesTask.State = TesState.SYSTEM_ERROR;
                        tesTask.SetFailureReason("BatchClientException", string.Join(",", batchClientException.Data.Values), batchClientException.Message, batchClientException.StackTrace);
                        logger.LogError(batchClientException, "TES task: {TesTask} BatchClientException.Message: {ExceptionMessage} {ExceptionData}", tesTask.Id, batchClientException.Message, string.Join(",", batchClientException?.Data?.Values));
                        break;

                    case BatchException batchException when batchException.InnerException is Microsoft.Azure.Batch.Protocol.Models.BatchErrorException batchErrorException && AzureBatchPoolCreationException.IsJobQuotaException(batchErrorException.Body.Code):
                        tesTask.SetWarning(batchErrorException.Body.Message.Value, Array.Empty<string>());
                        logger.LogInformation("Not enough job quota available for task Id {TesTask}. Reason: {BodyMessage}. Task will remain in queue.", tesTask.Id, batchErrorException.Body.Message.Value);
                        break;

                    case BatchException batchException when batchException.InnerException is Microsoft.Azure.Batch.Protocol.Models.BatchErrorException batchErrorException && AzureBatchPoolCreationException.IsPoolQuotaException(batchErrorException.Body.Code):
                        neededPools.Add(poolKey);
                        tesTask.SetWarning(batchErrorException.Body.Message.Value, Array.Empty<string>());
                        logger.LogInformation("Not enough pool quota available for task Id {TesTask}. Reason: {BodyMessage}. Task will remain in queue.", tesTask.Id, batchErrorException.Body.Message.Value);
                        break;

                    case Microsoft.Rest.Azure.CloudException cloudException when AzureBatchPoolCreationException.IsPoolQuotaException(cloudException.Body.Code):
                        neededPools.Add(poolKey);
                        tesTask.SetWarning(cloudException.Body.Message, Array.Empty<string>());
                        logger.LogInformation("Not enough pool quota available for task Id {TesTask}. Reason: {BodyMessage}. Task will remain in queue.", tesTask.Id, cloudException.Body.Message);
                        break;

                    default:
                        tesTask.State = TesState.SYSTEM_ERROR;
                        tesTask.SetFailureReason("UnknownError", $"{exception?.GetType().FullName}: {exception?.Message}", exception?.StackTrace);
                        logger.LogError(exception, "TES task: {TesTask} Exception: {ExceptionType}: {ExceptionMessage}", tesTask.Id, exception?.GetType().FullName, exception?.Message);
                        break;
                }
            }
        }

        /// <summary>
        /// Gets the current state of the Azure Batch task
        /// </summary>
        /// <param name="tesTask"><see cref="TesTask"/></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>A higher-level abstraction of the current state of the Azure Batch task</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1826:Do not use Enumerable methods on indexable collections", Justification = "FirstOrDefault() is straightforward, the alternative is less clear.")]
        private async ValueTask<CombinedBatchTaskInfo> GetBatchTaskStateAsync(TesTask tesTask, CancellationToken cancellationToken)
        {
            var azureBatchJobAndTaskState = await azureProxy.GetBatchJobAndTaskStateAsync(tesTask, cancellationToken);

            if (string.IsNullOrEmpty(azureBatchJobAndTaskState.PoolId) && !string.IsNullOrEmpty(tesTask.PoolId))
            {
                azureBatchJobAndTaskState.PoolId = tesTask.PoolId;
            }

            static IEnumerable<string> ConvertNodeErrorsToSystemLogItems(AzureBatchJobAndTaskState azureBatchJobAndTaskState)
            {
                var systemLogItems = new List<string>();

                if (azureBatchJobAndTaskState.NodeErrorCode is not null)
                {
                    systemLogItems.Add(azureBatchJobAndTaskState.NodeErrorCode);
                }

                if (azureBatchJobAndTaskState.NodeErrorDetails is not null)
                {
                    systemLogItems.AddRange(azureBatchJobAndTaskState.NodeErrorDetails);
                }

                return systemLogItems;
            }

            if (azureBatchJobAndTaskState.ActiveJobWithMissingAutoPool)
            {
                logger.LogWarning("Found active job without auto pool for TES task {TesTask}. Deleting the job and requeuing the task. BatchJobInfo: {BatchJobInfo}", tesTask.Id, JsonConvert.SerializeObject(azureBatchJobAndTaskState));
                return new CombinedBatchTaskInfo
                {
                    BatchTaskState = BatchTaskState.ActiveJobWithMissingAutoPool,
                    FailureReason = BatchTaskState.ActiveJobWithMissingAutoPool.ToString(),
                    Pool = azureBatchJobAndTaskState.PoolId
                };
            }

            if (azureBatchJobAndTaskState.MoreThanOneActiveJobOrTaskFound)
            {
                return new CombinedBatchTaskInfo
                {
                    BatchTaskState = BatchTaskState.MoreThanOneActiveJobOrTaskFound,
                    FailureReason = BatchTaskState.MoreThanOneActiveJobOrTaskFound.ToString(),
                    Pool = azureBatchJobAndTaskState.PoolId
                };
            }

            // Because a ComputeTask is not assigned to the compute node while the StartTask is running, IAzureProxy.GetBatchJobAndTaskStateAsync() does not see start task failures. Deal with that here.
            if (azureBatchJobAndTaskState.NodeState is null && azureBatchJobAndTaskState.JobState == JobState.Active && azureBatchJobAndTaskState.TaskState == TaskState.Active && !string.IsNullOrWhiteSpace(azureBatchJobAndTaskState.PoolId))
            {
                /*
                    * Priority order for assigning errors to TesTasks:
                    * 1. Node error found in GetBatchJobAndTaskStateAsync()
                    * 2. StartTask failure
                    * 3. NodeAllocation failure
                    */
                if (TryGetPool(azureBatchJobAndTaskState.PoolId, out var pool))
                {
                    if (!string.IsNullOrWhiteSpace(azureBatchJobAndTaskState.NodeErrorCode) || !ProcessStartTaskFailure(pool.PopNextStartTaskFailure()))
                    {
                        var resizeError = pool.PopNextResizeError();
                        if (resizeError is not null)
                        {
                            azureBatchJobAndTaskState.NodeAllocationFailed = true;
                            azureBatchJobAndTaskState.NodeErrorCode = resizeError.Code;
                            azureBatchJobAndTaskState.NodeErrorDetails = Enumerable.Repeat(resizeError.Message, string.IsNullOrWhiteSpace(resizeError.Message) ? 1 : 0).Concat(resizeError.Values?.Select(d => d.Value) ?? Enumerable.Empty<string>());
                        }
                    }
                }

                bool ProcessStartTaskFailure(IBatchPool.StartTaskFailureInformation failureInformation)
                {
                    if (failureInformation.TaskFailureInformation is not null)
                    {
                        azureBatchJobAndTaskState.NodeState = ComputeNodeState.StartTaskFailed;
                        azureBatchJobAndTaskState.NodeErrorCode = failureInformation.TaskFailureInformation.Code;
                        azureBatchJobAndTaskState.NodeErrorDetails = (failureInformation.TaskFailureInformation.Details?.Select(d => d.Value) ?? [])
                            .Append(failureInformation.NodeId);
                    }

                    return failureInformation.TaskFailureInformation is not null;
                }
            }

            if (TaskFailureInformationCodes.DiskFull.Equals(azureBatchJobAndTaskState.NodeErrorCode, StringComparison.OrdinalIgnoreCase))
            {
                azureBatchJobAndTaskState.NodeErrorDetails = (azureBatchJobAndTaskState.NodeErrorDetails ?? Enumerable.Empty<string>())
                    .Append($"Compute Node Error: {TaskFailureInformationCodes.DiskFull} Id: {azureBatchJobAndTaskState.NodeId}");
            }

            switch (azureBatchJobAndTaskState.JobState)
            {
                case null:
                case JobState.Deleting:
                    return new CombinedBatchTaskInfo
                    {
                        BatchTaskState = BatchTaskState.JobNotFound,
                        FailureReason = BatchTaskState.JobNotFound.ToString(),
                        Pool = azureBatchJobAndTaskState.PoolId
                    };
                case JobState.Active:
                    {
                        if (azureBatchJobAndTaskState.NodeAllocationFailed)
                        {
                            return new CombinedBatchTaskInfo
                            {
                                BatchTaskState = BatchTaskState.NodeAllocationFailed,
                                FailureReason = BatchTaskState.NodeAllocationFailed.ToString(),
                                SystemLogItems = ConvertNodeErrorsToSystemLogItems(azureBatchJobAndTaskState),
                                Pool = azureBatchJobAndTaskState.PoolId
                            };
                        }

                        if (azureBatchJobAndTaskState.NodeState == ComputeNodeState.Unusable)
                        {
                            return new CombinedBatchTaskInfo
                            {
                                BatchTaskState = BatchTaskState.NodeUnusable,
                                FailureReason = BatchTaskState.NodeUnusable.ToString(),
                                SystemLogItems = ConvertNodeErrorsToSystemLogItems(azureBatchJobAndTaskState),
                                Pool = azureBatchJobAndTaskState.PoolId
                            };
                        }

                        if (azureBatchJobAndTaskState.NodeState == ComputeNodeState.Preempted)
                        {
                            return new CombinedBatchTaskInfo
                            {
                                BatchTaskState = BatchTaskState.NodePreempted,
                                FailureReason = BatchTaskState.NodePreempted.ToString(),
                                SystemLogItems = ConvertNodeErrorsToSystemLogItems(azureBatchJobAndTaskState),
                                Pool = azureBatchJobAndTaskState.PoolId
                            };
                        }

                        if (azureBatchJobAndTaskState.NodeErrorCode is not null && !TaskState.Completed.Equals(azureBatchJobAndTaskState.TaskState))
                        {
                            return new CombinedBatchTaskInfo
                            {
                                BatchTaskState = BatchTaskState.NodeFailedDuringStartupOrExecution,
                                FailureReason = azureBatchJobAndTaskState.NodeErrorCode,
                                SystemLogItems = ConvertNodeErrorsToSystemLogItems(azureBatchJobAndTaskState),
                                Pool = azureBatchJobAndTaskState.PoolId
                            };
                        }

                        break;
                    }
                case JobState.Terminating:
                case JobState.Completed:
                    break;
                default:
                    throw new Exception($"Found batch job {tesTask.Id} in unexpected state: {azureBatchJobAndTaskState.JobState}");
            }

            switch (azureBatchJobAndTaskState.TaskState)
            {
                case null:
                    return new CombinedBatchTaskInfo
                    {
                        BatchTaskState = BatchTaskState.MissingBatchTask,
                        FailureReason = BatchTaskState.MissingBatchTask.ToString(),
                        Pool = azureBatchJobAndTaskState.PoolId
                    };
                case TaskState.Active:
                case TaskState.Preparing:
                    return new CombinedBatchTaskInfo
                    {
                        BatchTaskState = BatchTaskState.Initializing,
                        Pool = azureBatchJobAndTaskState.PoolId
                    };
                case TaskState.Running:
                    return new CombinedBatchTaskInfo
                    {
                        BatchTaskState = BatchTaskState.Running,
                        Pool = azureBatchJobAndTaskState.PoolId
                    };
                case TaskState.Completed:
                    if (azureBatchJobAndTaskState.TaskExitCode == 0 && azureBatchJobAndTaskState.TaskFailureInformation is null)
                    {
                        var metrics = await GetBatchNodeMetricsAndCromwellResultCodeAsync(tesTask, cancellationToken);

                        return new CombinedBatchTaskInfo
                        {
                            BatchTaskState = BatchTaskState.CompletedSuccessfully,
                            BatchTaskExitCode = azureBatchJobAndTaskState.TaskExitCode,
                            BatchTaskStartTime = metrics.TaskStartTime ?? azureBatchJobAndTaskState.TaskStartTime,
                            BatchTaskEndTime = metrics.TaskEndTime ?? azureBatchJobAndTaskState.TaskEndTime,
                            BatchNodeMetrics = metrics.BatchNodeMetrics,
                            CromwellRcCode = metrics.CromwellRcCode,
                            Pool = azureBatchJobAndTaskState.PoolId
                        };
                    }
                    else
                    {
                        logger.LogError("Task {TesTask} failed. ExitCode: {TaskExitCode}, BatchJobInfo: {BatchJobInfo}", tesTask.Id, azureBatchJobAndTaskState.TaskExitCode, JsonConvert.SerializeObject(azureBatchJobAndTaskState));

                        return new CombinedBatchTaskInfo
                        {
                            BatchTaskState = BatchTaskState.CompletedWithErrors,
                            FailureReason = azureBatchJobAndTaskState.TaskFailureInformation?.Code,
                            BatchTaskExitCode = azureBatchJobAndTaskState.TaskExitCode,
                            BatchTaskStartTime = azureBatchJobAndTaskState.TaskStartTime,
                            BatchTaskEndTime = azureBatchJobAndTaskState.TaskEndTime,
                            SystemLogItems = Enumerable.Empty<string>()
                                .Append($"Batch task ExitCode: {azureBatchJobAndTaskState.TaskExitCode}, Failure message: {azureBatchJobAndTaskState.TaskFailureInformation?.Message}")
                                .Concat(azureBatchJobAndTaskState.TaskFailureInformation?.Details?.Select(d => $"{d.Name}: {d.Value}") ?? Enumerable.Empty<string>()),
                            Pool = azureBatchJobAndTaskState.PoolId
                        };
                    }
                default:
                    throw new Exception($"Found batch task {tesTask.Id} in unexpected state: {azureBatchJobAndTaskState.TaskState}");
            }
        }

        /// <summary>
        /// Transitions the <see cref="TesTask"/> to the new state, based on the rules defined in the tesTaskStateTransitions list.
        /// </summary>
        /// <param name="tesTask">TES task</param>
        /// <param name="combinedBatchTaskInfo">Current Azure Batch task info</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>True if the TES task was changed.</returns>
        // When task is executed the following may be touched:
        // tesTask.Log[].SystemLog
        // tesTask.Log[].FailureReason
        // tesTask.Log[].CromwellResultCode
        // tesTask.Log[].BatchExecutionMetrics
        // tesTask.Log[].EndTime
        // tesTask.Log[].Log[].StdErr
        // tesTask.Log[].Log[].ExitCode
        // tesTask.Log[].Log[].StartTime
        // tesTask.Log[].Log[].EndTime
        private ValueTask<bool> HandleTesTaskTransitionAsync(TesTask tesTask, CombinedBatchTaskInfo combinedBatchTaskInfo, CancellationToken cancellationToken)
            => tesTaskStateTransitions
                .FirstOrDefault(m => (m.Condition is null || m.Condition(tesTask)) && (m.CurrentBatchTaskState is null || m.CurrentBatchTaskState == combinedBatchTaskInfo.BatchTaskState))
                ?.ActionAsync(tesTask, combinedBatchTaskInfo, cancellationToken) ?? ValueTask.FromResult(false);

        private async Task<CloudTask> ConvertTesTaskToBatchTaskUsingRunnerAsync(string taskId, TesTask task,
            CancellationToken cancellationToken)
        {
            ValidateTesTask(task);

            var nodeTaskCreationOptions = await GetNodeTaskConversionOptionsAsync(task, cancellationToken);

            var assets = await taskExecutionScriptingManager.PrepareBatchScriptAsync(task, nodeTaskCreationOptions, cancellationToken);

            var batchRunCommand = taskExecutionScriptingManager.ParseBatchRunCommand(assets);

            var cloudTask = new CloudTask(taskId, batchRunCommand)
            {
                Constraints = new(maxWallClockTime: poolLifetime, retentionTime: TimeSpan.Zero, maxTaskRetryCount: 0),
                UserIdentity = new(new AutoUserSpecification(elevationLevel: ElevationLevel.Admin, scope: AutoUserScope.Pool)),
            };

            return cloudTask;
        }

        private async Task<NodeTaskConversionOptions> GetNodeTaskConversionOptionsAsync(TesTask task, CancellationToken cancellationToken)
        {
            var nodeTaskCreationOptions = new NodeTaskConversionOptions(
                DefaultStorageAccountName: defaultStorageAccountName,
                AdditionalInputs: await GetAdditionalCromwellInputsAsync(task, cancellationToken),
                GlobalManagedIdentity: globalManagedIdentity,
                DrsHubApiHost: drsHubApiHost
            );
            return nodeTaskCreationOptions;
        }

        private async Task<List<TesInput>> GetAdditionalCromwellInputsAsync(TesTask task, CancellationToken cancellationToken)
        {
            // TODO: Cromwell bug: Cromwell command write_tsv() generates a file in the execution directory, for example execution/write_tsv_3922310b441805fc43d52f293623efbc.tmp. These are not passed on to TES inputs.
            // WORKAROUND: Get the list of files in the execution directory and add them to task inputs.
            // TODO: Verify whether this workaround is still needed.
            List<TesInput> additionalInputs = [];

            if (task.IsCromwell())
            {
                additionalInputs =
                    await GetExistingBlobsInCromwellStorageLocationAsTesInputsAsync(task, cancellationToken);
            }

            return additionalInputs;
        }

        private async Task<List<TesInput>> GetExistingBlobsInCromwellStorageLocationAsTesInputsAsync(TesTask task, CancellationToken cancellationToken)
        {
            var additionalInputFiles = new List<TesInput>();
            var metadata = task.GetCromwellMetadata();

            var cromwellExecutionDirectory = (Uri.TryCreate(metadata.CromwellRcUri, UriKind.Absolute, out var uri) && !uri.IsFile)
                ? GetParentUrl(metadata.CromwellRcUri)
                : $"/{GetParentUrl(metadata.CromwellRcUri)}";

            var executionDirectoryUri = await storageAccessProvider.MapLocalPathToSasUrlAsync(cromwellExecutionDirectory,
                cancellationToken, getContainerSas: true);

            if (executionDirectoryUri is not null)
            {
                var blobsInExecutionDirectory =
                    (await azureProxy.ListBlobsAsync(executionDirectoryUri, cancellationToken)).ToList();
                var scriptBlob =
                    blobsInExecutionDirectory.FirstOrDefault(b => b.Name.EndsWith($"/{CromwellScriptFileName}"));
                var commandScript =
                    task.Inputs?.FirstOrDefault(b => "commandScript".Equals(b.Name));

                if (scriptBlob is not null)
                {
                    blobsInExecutionDirectory.Remove(scriptBlob);
                }

                if (commandScript is not null)
                {
                    var commandScriptPathParts = commandScript.Path.Split('/').ToList();
                    additionalInputFiles = await blobsInExecutionDirectory
                        .Select(b => (Path: $"/{metadata.CromwellExecutionDir.TrimStart('/')}/{b.Name.Split('/').Last()}",
                            b.Uri))
                        .ToAsyncEnumerable()
                        .SelectAwait(async b => new TesInput
                        {
                            Path = b.Path,
                            Url = (await storageAccessProvider.MapLocalPathToSasUrlAsync(b.Uri.AbsoluteUri,
                                cancellationToken, getContainerSas: true)).AbsoluteUri,
                            Name = Path.GetFileName(b.Path),
                            Type = TesFileType.FILE
                        })
                        .ToListAsync(cancellationToken);
                }
            }

            return additionalInputFiles;
        }

        private void ValidateTesTask(TesTask task)
        {
            ArgumentNullException.ThrowIfNull(task);

            task.Inputs?.ForEach(input => ValidateTesTaskInput(input, task));
        }

        private void ValidateTesTaskInput(TesInput inputFile, TesTask tesTask)
        {
            if (string.IsNullOrWhiteSpace(inputFile.Path) || !inputFile.Path.StartsWith("/"))
            {
                throw new TesException("InvalidInputFilePath", $"Unsupported input path '{inputFile.Path}' for task Id {tesTask.Id}. Must start with '/'.");
            }

            if (inputFile.Url is not null && inputFile.Content is not null)
            {
                throw new TesException("InvalidInputFilePath", "Input Url and Content cannot be both set");
            }

            if (inputFile.Url is null && inputFile.Content is null)
            {
                throw new TesException("InvalidInputFilePath", "One of Input Url or Content must be set");
            }

            if (inputFile.Type == TesFileType.DIRECTORY)
            {
                throw new TesException("InvalidInputFilePath", "Directory input is not supported.");
            }
        }

        enum StartScriptVmFamilies
        {
            standardLSFamily,
            standardLSv2Family,
            standardLSv3Family,
            standardLASv3Family,
        }

        /// <summary>
        /// Constructs a universal Azure Start Task instance
        /// </summary>
        /// <param name="poolId">Pool Id</param>
        /// <param name="machineConfiguration">A <see cref="BatchModels.VirtualMachineConfiguration"/> describing the OS of the pool's nodes.</param>
        /// <param name="vmFamily"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        /// <remarks>This method also mitigates errors associated with docker daemons that are not configured to place their filesystem assets on the data drive.</remarks>
        private async Task<BatchModels.StartTask> GetStartTaskAsync(string poolId, BatchModels.VirtualMachineConfiguration machineConfiguration, string vmFamily, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(poolId);
            ArgumentNullException.ThrowIfNull(machineConfiguration);
            ArgumentNullException.ThrowIfNull(vmFamily);

            var globalStartTaskConfigured = !string.IsNullOrWhiteSpace(globalStartTaskPath);

            var globalStartTaskSasUrl = globalStartTaskConfigured
                ? await storageAccessProvider.MapLocalPathToSasUrlAsync(globalStartTaskPath, cancellationToken, sasTokenDuration: BatchPoolService.RunInterval.Multiply(2).Add(poolLifetime).Add(TimeSpan.FromMinutes(15)))
                : default;

            if (globalStartTaskSasUrl is not null)
            {
                if (!await azureProxy.BlobExistsAsync(globalStartTaskSasUrl, cancellationToken))
                {
                    globalStartTaskSasUrl = default;
                    globalStartTaskConfigured = false;
                }
            }
            else
            {
                globalStartTaskConfigured = false;
            }

            // https://learn.microsoft.com/azure/batch/batch-docker-container-workloads#linux-support
            var dockerConfigured = machineConfiguration.ImageReference.Publisher.Equals("microsoft-azure-batch", StringComparison.OrdinalIgnoreCase)
                && (machineConfiguration.ImageReference.Offer.StartsWith("ubuntu-server-container", StringComparison.OrdinalIgnoreCase) || machineConfiguration.ImageReference.Offer.StartsWith("centos-container", StringComparison.OrdinalIgnoreCase));

            StringBuilder cmd = new("#!/bin/sh\n");
            cmd.Append($"mkdir -p {BatchNodeSharedEnvVar} && {CreateWgetDownloadCommand(await storageAccessProvider.GetInternalTesBlobUrlAsync(NodeTaskRunnerFilename, cancellationToken), $"{BatchNodeSharedEnvVar}/{NodeTaskRunnerFilename}", setExecutable: true)}");

            if (!dockerConfigured)
            {
                var packageInstallScript = machineConfiguration.NodeAgentSkuId switch
                {
                    var s when s.StartsWith("batch.node.ubuntu ", StringComparison.OrdinalIgnoreCase) => "echo \"Ubuntu OS detected\"",
                    var s when s.StartsWith("batch.node.centos ", StringComparison.OrdinalIgnoreCase) => "sudo yum install epel-release -y && sudo yum update -y && sudo yum install -y wget",
                    _ => throw new InvalidOperationException($"Unrecognized OS. Please send open an issue @ 'https://github.com/microsoft/ga4gh-tes/issues' with this message ({machineConfiguration.NodeAgentSkuId})")
                };

                var script = "config-docker.sh";
                cmd.Append($" && {CreateWgetDownloadCommand(await UploadScriptAsync(script, new((await ReadScript("config-docker.sh")).Replace("{PackageInstalls}", packageInstallScript))), $"{BatchNodeTaskWorkingDirEnvVar}/{script}", setExecutable: true)} && {BatchNodeTaskWorkingDirEnvVar}/{script}");
            }

            var vmFamilyStartupScript = (Enum.TryParse(typeof(StartScriptVmFamilies), vmFamily, out var family) ? family : default) switch
            {
                StartScriptVmFamilies.standardLSFamily => @"config-nvme.sh",
                StartScriptVmFamilies.standardLSv2Family => @"config-nvme.sh",
                StartScriptVmFamilies.standardLSv3Family => @"config-nvme.sh",
                StartScriptVmFamilies.standardLASv3Family => @"config-nvme.sh",
                _ => null
            };

            if (!string.IsNullOrWhiteSpace(vmFamilyStartupScript))
            {
                var script = "config-vmfamily.sh";
                // TODO: optimize this by uploading all vmfamily scripts when uploading runner binary rather then for each individual pool
                cmd.Append($" && {CreateWgetDownloadCommand(await UploadScriptAsync(script, new(await ReadScript(vmFamilyStartupScript))), $"{BatchNodeTaskWorkingDirEnvVar}/{script}", setExecutable: true)} && {BatchNodeTaskWorkingDirEnvVar}/{script}");
            }

            if (globalStartTaskConfigured)
            {
                cmd.Append($" && {CreateWgetDownloadCommand(globalStartTaskSasUrl, $"{BatchNodeTaskWorkingDirEnvVar}/global-{StartTaskScriptFilename}", setExecutable: true)} && {BatchNodeTaskWorkingDirEnvVar}/global-{StartTaskScriptFilename}");
            }

            return new()
            {
                CommandLine = $"/bin/sh -c \"{CreateWgetDownloadCommand(await UploadScriptAsync(StartTaskScriptFilename, cmd), $"{BatchNodeTaskWorkingDirEnvVar}/{StartTaskScriptFilename}", true)} && {BatchNodeTaskWorkingDirEnvVar}/{StartTaskScriptFilename}\"",
                UserIdentity = new BatchModels.UserIdentity(autoUser: new(elevationLevel: BatchModels.ElevationLevel.Admin, scope: BatchModels.AutoUserScope.Pool)),
                MaxTaskRetryCount = 1,
                WaitForSuccess = true
            };

            async ValueTask<Uri> UploadScriptAsync(string name, StringBuilder content)
            {
                content.AppendLinuxLine(string.Empty);
                var path = $"/pools/{poolId}/{name}";
                var url = await storageAccessProvider.GetInternalTesBlobUrlAsync(path, cancellationToken);
                await azureProxy.UploadBlobAsync(url, content.ToString(), cancellationToken);
                content.Clear();
                return url;
            }

            async ValueTask<string> ReadScript(string name)
            {
                var path = Path.Combine(AppContext.BaseDirectory, "scripts", name);
                return await File.ReadAllTextAsync(path, cancellationToken);
            }
        }

        /// <summary>
        /// Generate the BatchPoolIdentity object
        /// </summary>
        /// <param name="identities"></param>
        /// <returns></returns>
        private static BatchModels.BatchPoolIdentity GetBatchPoolIdentity(string[] identities)
            => identities is null || !identities.Any() ? null : new(BatchModels.PoolIdentityType.UserAssigned, identities.ToDictionary(identity => identity, _ => new BatchModels.UserAssignedIdentities()));

        /// <summary>
        /// Generate the PoolSpecification for the needed pool.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="displayName"></param>
        /// <param name="poolIdentity"></param>
        /// <param name="vmSize"></param>
        /// <param name="vmFamily"></param>
        /// <param name="preemptable"></param>
        /// <param name="nodeInfo"></param>
        /// <param name="encryptionAtHostSupported">VM supports encryption at host.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>A <see cref="BatchModels.Pool"/>.</returns>
        /// <remarks>
        /// Devs: Any changes to any properties set in this method will require corresponding changes to all classes implementing <see cref="Management.Batch.IBatchPoolManager"/> along with possibly any systems they call, with the possible exception of <seealso cref="Management.Batch.ArmBatchPoolManager"/>.
        /// </remarks>
        private async ValueTask<BatchModels.Pool> GetPoolSpecification(string name, string displayName, BatchModels.BatchPoolIdentity poolIdentity, string vmSize, string vmFamily, bool preemptable, BatchNodeInfo nodeInfo, bool? encryptionAtHostSupported, CancellationToken cancellationToken)
        {
            // TODO: (perpetually) add new properties we set in the future on <see cref="PoolSpecification"/> and/or its contained objects, if possible. When not, update CreateAutoPoolModePoolInformation().

            ValidateString(name, nameof(name), 64);
            ValidateString(displayName, nameof(displayName), 1024);

            var vmConfig = new BatchModels.VirtualMachineConfiguration(
                imageReference: new BatchModels.ImageReference(
                    publisher: nodeInfo.BatchImagePublisher,
                    offer: nodeInfo.BatchImageOffer,
                    sku: nodeInfo.BatchImageSku,
                    version: nodeInfo.BatchImageVersion),
                nodeAgentSkuId: nodeInfo.BatchNodeAgentSkuId);

            if (encryptionAtHostSupported ?? false)
            {
                vmConfig.DiskEncryptionConfiguration = new(
                    targets: new List<BatchModels.DiskEncryptionTarget> { BatchModels.DiskEncryptionTarget.OsDisk, BatchModels.DiskEncryptionTarget.TemporaryDisk }
                );
            }

            var poolSpecification = new BatchModels.Pool(name: name, displayName: displayName, identity: poolIdentity, vmSize: vmSize)
            {
                ScaleSettings = new(autoScale: new(BatchPool.AutoPoolFormula(preemptable, 1), BatchPool.AutoScaleEvaluationInterval)),
                DeploymentConfiguration = new(virtualMachineConfiguration: vmConfig),
                //ApplicationPackages = ,
                StartTask = await GetStartTaskAsync(name, vmConfig, vmFamily, cancellationToken),
                TargetNodeCommunicationMode = BatchModels.NodeCommunicationMode.Simplified,
            };

            if (!string.IsNullOrEmpty(batchNodesSubnetId))
            {
                poolSpecification.NetworkConfiguration = new()
                {
                    PublicIPAddressConfiguration = new(provision: disableBatchNodesPublicIpAddress ? BatchModels.IPAddressProvisioningType.NoPublicIPAddresses : BatchModels.IPAddressProvisioningType.BatchManaged),
                    SubnetId = batchNodesSubnetId
                };
            }

            return poolSpecification;

            static void ValidateString(string value, string name, int length)
            {
                ArgumentNullException.ThrowIfNull(value, name);
                if (value.Length > length) throw new ArgumentException($"{name} exceeds maximum length {length}", name);
            }
        }

        /// <summary>
        /// Gets the cheapest available VM size that satisfies the <see cref="TesTask"/> execution requirements
        /// </summary>
        /// <param name="tesTask"><see cref="TesTask"/></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="forcePreemptibleVmsOnly">Force consideration of preemptible virtual machines only.</param>
        /// <returns>The virtual machine info</returns>
        public async Task<VirtualMachineInformation> GetVmSizeAsync(TesTask tesTask, CancellationToken cancellationToken, bool forcePreemptibleVmsOnly = false)
        {
            var allowedVmSizes = await allowedVmSizesService.GetAllowedVmSizes(cancellationToken);
            bool allowedVmSizesFilter(VirtualMachineInformation vm) => allowedVmSizes is null || !allowedVmSizes.Any() || allowedVmSizes.Contains(vm.VmSize, StringComparer.OrdinalIgnoreCase) || allowedVmSizes.Contains(vm.VmFamily, StringComparer.OrdinalIgnoreCase);

            var tesResources = tesTask.Resources;

            var previouslyFailedVmSizes = tesTask.Logs?
                .Where(log => log.FailureReason == BatchTaskState.NodeAllocationFailed.ToString() && log.VirtualMachineInfo?.VmSize is not null)
                .Select(log => log.VirtualMachineInfo.VmSize)
                .Distinct()
                .ToList();

            var virtualMachineInfoList = await skuInformationProvider.GetVmSizesAndPricesAsync(azureProxy.GetArmRegion(), cancellationToken);
            var preemptible = forcePreemptibleVmsOnly || usePreemptibleVmsOnly || (tesResources?.Preemptible).GetValueOrDefault(true);

            var eligibleVms = new List<VirtualMachineInformation>();
            var noVmFoundMessage = string.Empty;

            var vmSize = tesResources?.GetBackendParameterValue(TesResources.SupportedBackendParameters.vm_size);

            if (!string.IsNullOrWhiteSpace(vmSize))
            {
                eligibleVms = virtualMachineInfoList
                    .Where(vm =>
                        vm.LowPriority == preemptible
                        && vm.VmSize.Equals(vmSize, StringComparison.OrdinalIgnoreCase))
                    .ToList();

                noVmFoundMessage = $"No VM (out of {virtualMachineInfoList.Count}) available with the required resources (vmsize: {vmSize}, preemptible: {preemptible}) for task id {tesTask.Id}.";
            }
            else
            {
                var requiredNumberOfCores = (tesResources?.CpuCores).GetValueOrDefault(DefaultCoreCount);
                var requiredMemoryInGB = (tesResources?.RamGb).GetValueOrDefault(DefaultMemoryGb);
                var requiredDiskSizeInGB = (tesResources?.DiskGb).GetValueOrDefault(DefaultDiskGb);

                eligibleVms = virtualMachineInfoList
                    .Where(vm =>
                        vm.LowPriority == preemptible
                        && vm.VCpusAvailable >= requiredNumberOfCores
                        && vm.MemoryInGiB >= requiredMemoryInGB
                        && vm.ResourceDiskSizeInGiB >= requiredDiskSizeInGB)
                    .ToList();

                noVmFoundMessage = $"No VM (out of {virtualMachineInfoList.Count}) available with the required resources (cores: {requiredNumberOfCores}, memory: {requiredMemoryInGB} GB, disk: {requiredDiskSizeInGB} GB, preemptible: {preemptible}) for task id {tesTask.Id}.";
            }


            var coreQuota = await quotaVerifier
                .GetBatchQuotaProvider()
                .GetVmCoreQuotaAsync(preemptible, cancellationToken);

            var selectedVm = eligibleVms
                .Where(allowedVmSizesFilter)
                .Where(vm => IsThereSufficientCoreQuota(coreQuota, vm))
                .Where(vm =>
                    !(previouslyFailedVmSizes?.Contains(vm.VmSize, StringComparer.OrdinalIgnoreCase) ?? false))
                .MinBy(vm => vm.PricePerHour);

            if (!preemptible && selectedVm is not null)
            {
                var idealVm = eligibleVms
                    .Where(allowedVmSizesFilter)
                    .Where(vm => !(previouslyFailedVmSizes?.Contains(vm.VmSize, StringComparer.OrdinalIgnoreCase) ?? false))
                    .MinBy(x => x.PricePerHour);

                if (selectedVm.PricePerHour >= idealVm.PricePerHour * 2)
                {
                    tesTask.SetWarning("UsedLowPriorityInsteadOfDedicatedVm",
                        $"This task ran on low priority machine because dedicated quota was not available for VM Series '{idealVm.VmFamily}'.",
                        $"Increase the quota for VM Series '{idealVm.VmFamily}' to run this task on a dedicated VM. Please submit an Azure Support request to increase your quota: {AzureSupportUrl}");

                    return await GetVmSizeAsync(tesTask, cancellationToken, true);
                }
            }

            if (selectedVm is not null)
            {
                return selectedVm;
            }

            if (!eligibleVms.Any())
            {
                noVmFoundMessage += $" There are no VM sizes that match the requirements. Review the task resources.";
            }

            if (previouslyFailedVmSizes is not null)
            {
                noVmFoundMessage += $" The following VM sizes were excluded from consideration because of {BatchTaskState.NodeAllocationFailed} error(s) on previous attempts: {string.Join(", ", previouslyFailedVmSizes)}.";
            }

            var vmsExcludedByTheAllowedVmsConfiguration = eligibleVms.Except(eligibleVms.Where(allowedVmSizesFilter)).Count();

            if (vmsExcludedByTheAllowedVmsConfiguration > 0)
            {
                noVmFoundMessage += $" Note that {vmsExcludedByTheAllowedVmsConfiguration} VM(s), suitable for this task, were excluded by the allowed-vm-sizes configuration. Consider expanding the list of allowed VM sizes.";
            }

            throw new AzureBatchVirtualMachineAvailabilityException(noVmFoundMessage.Trim());
        }

        private static bool IsThereSufficientCoreQuota(BatchVmCoreQuota coreQuota, VirtualMachineInformation vm)
        {
            if (coreQuota.IsLowPriority || !coreQuota.IsDedicatedAndPerVmFamilyCoreQuotaEnforced)
            {
                return coreQuota.NumberOfCores >= vm.VCpusAvailable;
            }

            var result = coreQuota.DedicatedCoreQuotas?.FirstOrDefault(q => q.VmFamilyName.Equals(vm.VmFamily, StringComparison.OrdinalIgnoreCase));

            if (result is null)
            {
                return false;
            }

            return result?.CoreQuota >= vm.VCpusAvailable;
        }

        private async Task<(Tes.Models.BatchNodeMetrics BatchNodeMetrics, DateTimeOffset? TaskStartTime, DateTimeOffset? TaskEndTime, int? CromwellRcCode)> GetBatchNodeMetricsAndCromwellResultCodeAsync(TesTask tesTask, CancellationToken cancellationToken)
        {
            var bytesInGB = Math.Pow(1000, 3);
            var kiBInGB = Math.Pow(1000, 3) / 1024;

            static double? GetDurationInSeconds(Dictionary<string, string> dict, string startKey, string endKey)
            {
                return TryGetValueAsDateTimeOffset(dict, startKey, out var startTime) && TryGetValueAsDateTimeOffset(dict, endKey, out var endTime)
                    ? endTime.Subtract(startTime).TotalSeconds
                    : (double?)null;
            }

            static bool TryGetValueAsDateTimeOffset(Dictionary<string, string> dict, string key, out DateTimeOffset result)
            {
                result = default;
                return dict.TryGetValue(key, out var valueAsString) && DateTimeOffset.TryParse(valueAsString, out result);
            }

            static bool TryGetValueAsDouble(Dictionary<string, string> dict, string key, out double result)
            {
                result = default;
                return dict.TryGetValue(key, out var valueAsString) && double.TryParse(valueAsString, out result);
            }

            BatchNodeMetrics batchNodeMetrics = null;
            DateTimeOffset? taskStartTime = null;
            DateTimeOffset? taskEndTime = null;
            int? cromwellRcCode = null;

            try
            {
                if (tesTask.IsCromwell())
                {
                    var cromwellRcContent = await storageAccessProvider.DownloadBlobAsync(tesTask.GetCromwellMetadata().CromwellRcUri, cancellationToken);

                    if (cromwellRcContent is not null && int.TryParse(cromwellRcContent, out var temp))
                    {
                        cromwellRcCode = temp;
                    }
                }

                var metricsUrl = await storageAccessProvider.GetInternalTesTaskBlobUrlAsync(tesTask, "metrics.txt", cancellationToken);
                var metricsContent = await storageAccessProvider.DownloadBlobAsync(metricsUrl, cancellationToken);

                if (metricsContent is not null)
                {
                    try
                    {
                        var metrics = DelimitedTextToDictionary(metricsContent.Trim());

                        var diskSizeInGB = TryGetValueAsDouble(metrics, "DiskSizeInKiB", out var diskSizeInKiB) ? diskSizeInKiB / kiBInGB : (double?)null;
                        var diskUsedInGB = TryGetValueAsDouble(metrics, "DiskUsedInKiB", out var diskUsedInKiB) ? diskUsedInKiB / kiBInGB : (double?)null;

                        batchNodeMetrics = new BatchNodeMetrics
                        {
                            BlobXferImagePullDurationInSeconds = GetDurationInSeconds(metrics, "BlobXferPullStart", "BlobXferPullEnd"),
                            ExecutorImagePullDurationInSeconds = GetDurationInSeconds(metrics, "ExecutorPullStart", "ExecutorPullEnd"),
                            ExecutorImageSizeInGB = TryGetValueAsDouble(metrics, "ExecutorImageSizeInBytes", out var executorImageSizeInBytes) ? executorImageSizeInBytes / bytesInGB : (double?)null,
                            FileDownloadDurationInSeconds = GetDurationInSeconds(metrics, "DownloadStart", "DownloadEnd"),
                            FileDownloadSizeInGB = TryGetValueAsDouble(metrics, "FileDownloadSizeInBytes", out var fileDownloadSizeInBytes) ? fileDownloadSizeInBytes / bytesInGB : (double?)null,
                            ExecutorDurationInSeconds = GetDurationInSeconds(metrics, "ExecutorStart", "ExecutorEnd"),
                            FileUploadDurationInSeconds = GetDurationInSeconds(metrics, "UploadStart", "UploadEnd"),
                            FileUploadSizeInGB = TryGetValueAsDouble(metrics, "FileUploadSizeInBytes", out var fileUploadSizeInBytes) ? fileUploadSizeInBytes / bytesInGB : (double?)null,
                            DiskUsedInGB = diskUsedInGB,
                            DiskUsedPercent = diskUsedInGB.HasValue && diskSizeInGB.HasValue && diskSizeInGB > 0 ? (float?)(diskUsedInGB / diskSizeInGB * 100) : null,
                            VmCpuModelName = metrics.GetValueOrDefault("VmCpuModelName")
                        };

                        taskStartTime = TryGetValueAsDateTimeOffset(metrics, "BlobXferPullStart", out var startTime) ? (DateTimeOffset?)startTime : null;
                        taskEndTime = TryGetValueAsDateTimeOffset(metrics, "UploadEnd", out var endTime) ? (DateTimeOffset?)endTime : null;
                    }
                    catch (Exception ex)
                    {
                        logger.LogError("Failed to parse metrics for task {TesTask}. Error: {ExceptionMessage}", tesTask.Id, ex.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogError("Failed to get batch node metrics for task {TesTask}. Error: {ExceptionMessage}", tesTask.Id, ex.Message);
            }

            return (batchNodeMetrics, taskStartTime, taskEndTime, cromwellRcCode);
        }

        private static Dictionary<string, string> DelimitedTextToDictionary(string text, string fieldDelimiter = "=", string rowDelimiter = "\n")
            => text.Split(rowDelimiter)
                .Select(line => { var parts = line.Split(fieldDelimiter); return new KeyValuePair<string, string>(parts[0], parts[1]); })
                .ToDictionary(kv => kv.Key, kv => kv.Value);

        /// <summary>
        /// Class that captures how <see cref="TesTask"/> transitions from current state to the new state, given the current Batch task state and optional condition. 
        /// Transitions typically include an action that needs to run in order for the task to move to the new state.
        /// </summary>
        private class TesTaskStateTransition
        {
            public TesTaskStateTransition(Func<TesTask, bool> condition, BatchTaskState? batchTaskState, string alternateSystemLogItem, Func<TesTask, CombinedBatchTaskInfo, CancellationToken, Task> asyncAction)
                : this(condition, batchTaskState, alternateSystemLogItem, asyncAction, null)
            { }

            public TesTaskStateTransition(Func<TesTask, bool> condition, BatchTaskState? batchTaskState, string alternateSystemLogItem, Action<TesTask, CombinedBatchTaskInfo> action)
                : this(condition, batchTaskState, alternateSystemLogItem, null, action)
            {
            }

            private TesTaskStateTransition(Func<TesTask, bool> condition, BatchTaskState? batchTaskState, string alternateSystemLogItem, Func<TesTask, CombinedBatchTaskInfo, CancellationToken, Task> asyncAction, Action<TesTask, CombinedBatchTaskInfo> action)
            {
                Condition = condition;
                CurrentBatchTaskState = batchTaskState;
                AlternateSystemLogItem = alternateSystemLogItem;
                AsyncAction = asyncAction;
                Action = action;
            }

            public Func<TesTask, bool> Condition { get; }
            public BatchTaskState? CurrentBatchTaskState { get; }
            private string AlternateSystemLogItem { get; }
            private Func<TesTask, CombinedBatchTaskInfo, CancellationToken, Task> AsyncAction { get; }
            private Action<TesTask, CombinedBatchTaskInfo> Action { get; }

            /// <summary>
            /// Calls <see cref="Action"/> and/or <see cref="AsyncAction"/>.
            /// </summary>
            /// <param name="tesTask"></param>
            /// <param name="combinedBatchTaskInfo"></param>
            /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
            /// <returns>True an action was called, otherwise False.</returns>
            public async ValueTask<bool> ActionAsync(TesTask tesTask, CombinedBatchTaskInfo combinedBatchTaskInfo, CancellationToken cancellationToken)
            {
                combinedBatchTaskInfo.AlternateSystemLogItem = AlternateSystemLogItem;
                var tesTaskChanged = false;

                if (AsyncAction is not null)
                {
                    await AsyncAction(tesTask, combinedBatchTaskInfo, cancellationToken);
                    tesTaskChanged = true;
                }

                if (Action is not null)
                {
                    Action(tesTask, combinedBatchTaskInfo);
                    tesTaskChanged = true;
                }

                return tesTaskChanged;
            }
        }

        private class CombinedBatchTaskInfo
        {
            public BatchTaskState BatchTaskState { get; set; }
            public BatchNodeMetrics BatchNodeMetrics { get; set; }
            public string FailureReason { get; set; }
            public DateTimeOffset? BatchTaskStartTime { get; set; }
            public DateTimeOffset? BatchTaskEndTime { get; set; }
            public int? BatchTaskExitCode { get; set; }
            public int? CromwellRcCode { get; set; }
            public IEnumerable<string> SystemLogItems { get; set; }
            public string Pool { get; set; }
            public string AlternateSystemLogItem { get; set; }
        }
    }
}
