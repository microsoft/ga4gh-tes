// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.ResourceManager.Batch;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using CommonUtilities;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Primitives;
using Tes.Extensions;
using TesApi.Web.Events;
using TesApi.Web.Extensions;
using TesApi.Web.Management;
using TesApi.Web.Management.Batch;
using TesApi.Web.Management.Models.Quotas;
using TesApi.Web.Runner;
using TesApi.Web.Storage;
using BatchModels = Azure.ResourceManager.Batch.Models;
using CloudTaskId = TesApi.Web.IBatchScheduler.CloudTaskId;
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
    /// Orchestrates <see cref="TesTask"/>s on Azure Batch
    /// </summary>
    public partial class BatchScheduler : IBatchScheduler
    {
        /// <summary>
        /// Minimum lifetime of a <see cref="CloudTask"/>.
        /// </summary>
        // https://learn.microsoft.com/azure/batch/best-practices#manage-task-lifetime
        public static TimeSpan BatchDeleteNewTaskWorkaroundTimeSpan { get; } = TimeSpan.FromMinutes(10);
        internal const string PoolDeprecated = "CoA-TES-HostName";
        internal const string PoolMetadata = "CoA-TES-Metadata";

        /// <summary>
        /// Name of <c>$</c> prefixed environment variable to place resources shared by all tasks on each compute node in a pool.
        /// </summary>
        public const string BatchNodeSharedEnvVar = "${AZ_BATCH_NODE_SHARED_DIR}";

        /// <summary>
        /// Name of <c>$</c> prefixed environment variable of the working directory of the running task.
        /// </summary>
        public const string BatchNodeTaskWorkingDirEnvVar = "$AZ_BATCH_TASK_WORKING_DIR";

        internal const string NodeTaskRunnerFilename = "tes-runner";

        internal static TimeSpan QueuedTesTaskTaskGroupGatherWindow = TimeSpan.FromSeconds(10);
        internal static TimeSpan QueuedTesTaskPoolGroupGatherWindow = TaskScheduler.BatchRunInterval;

        private const string AzureSupportUrl = "https://portal.azure.com/#blade/Microsoft_Azure_Support/HelpAndSupportBlade/newsupportrequest";
        private const int PoolKeyLength = 55; // 64 max pool name length - 9 chars generating unique pool names
        private const int DefaultCoreCount = 1;
        private const int DefaultMemoryGb = 2;
        private const int DefaultDiskGb = 10;
        private const string StartTaskScriptFilename = "start-task.sh";
        private const string NodeTaskRunnerMD5HashFilename = NodeTaskRunnerFilename + ".md5";
        private readonly ILogger logger;
        private readonly IAzureProxy azureProxy;
        private readonly IBatchPoolManager batchPoolManager;
        private readonly IStorageAccessProvider storageAccessProvider;
        private readonly IBatchQuotaVerifier quotaVerifier;
        private readonly IBatchSkuInformationProvider skuInformationProvider;
        private readonly IReadOnlyList<TesTaskStateTransition> tesTaskStateTransitions;
        private readonly bool usePreemptibleVmsOnly;
        private readonly string batchNodesSubnetId;
        private readonly bool batchNodesSetContentMd5OnUpload;
        private readonly bool disableBatchNodesPublicIpAddress;
        private readonly TimeSpan poolLifetime;
        private readonly TimeSpan taskMaxWallClockTime;
        private readonly BatchNodeInfo gen2BatchNodeInfo;
        private readonly BatchNodeInfo gen1BatchNodeInfo;
        private readonly string defaultStorageAccountName;
        private readonly string globalStartTaskPath;
        private readonly string globalManagedIdentity;
        private readonly string batchPrefix;
        private readonly Func<IBatchPool> batchPoolFactory;
        private readonly IAllowedVmSizesService allowedVmSizesService;
        private readonly TaskExecutionScriptingManager taskExecutionScriptingManager;
        private readonly string runnerMD5;
        private readonly string drsHubApiHost;
        private readonly IActionIdentityProvider actionIdentityProvider;

        /// <summary>
        /// Constructor for <see cref="BatchScheduler"/>.
        /// </summary>
        /// <param name="logger">Logger <see cref="ILogger"/>.</param>
        /// <param name="batchGen1Options">Configuration of <see cref="Options.BatchImageGeneration1Options"/>.</param>
        /// <param name="batchGen2Options">Configuration of <see cref="Options.BatchImageGeneration2Options"/>.</param>
        /// <param name="drsHubOptions">Configuration of <see cref="Options.DrsHubOptions"/>.</param>
        /// <param name="storageOptions">Configuration of <see cref="Options.StorageOptions"/>.</param>
        /// <param name="batchNodesOptions">Configuration of <see cref="Options.BatchNodesOptions"/>.</param>
        /// <param name="batchSchedulingOptions">Configuration of <see cref="Options.BatchSchedulingOptions"/>.</param>
        /// <param name="azureProxy">Azure proxy <see cref="IAzureProxy"/>.</param>
        /// <param name="batchPoolManager">Azure batch pool management proxy.</param>
        /// <param name="storageAccessProvider">Storage access provider <see cref="IStorageAccessProvider"/>.</param>
        /// <param name="quotaVerifier">Quota verifier <see cref="IBatchQuotaVerifier"/>.</param>
        /// <param name="skuInformationProvider">Sku information provider <see cref="IBatchSkuInformationProvider"/>.</param>
        /// <param name="poolFactory">Batch pool factory <see cref="IBatchPool"/>.</param>
        /// <param name="allowedVmSizesService">Service to get allowed vm sizes.</param>
        /// <param name="taskExecutionScriptingManager"><see cref="TaskExecutionScriptingManager"/>.</param>
        /// <param name="actionIdentityProvider"><see cref="IActionIdentityProvider"/>.</param>
        public BatchScheduler(
            ILogger<BatchScheduler> logger,
            IOptions<Options.BatchImageGeneration1Options> batchGen1Options,
            IOptions<Options.BatchImageGeneration2Options> batchGen2Options,
            IOptions<Options.DrsHubOptions> drsHubOptions,
            IOptions<Options.StorageOptions> storageOptions,
            IOptions<Options.BatchNodesOptions> batchNodesOptions,
            IOptions<Options.BatchSchedulingOptions> batchSchedulingOptions,
            IAzureProxy azureProxy,
            IBatchPoolManager batchPoolManager,
            IStorageAccessProvider storageAccessProvider,
            IBatchQuotaVerifier quotaVerifier,
            IBatchSkuInformationProvider skuInformationProvider,
            Func<IBatchPool> poolFactory,
            IAllowedVmSizesService allowedVmSizesService,
            TaskExecutionScriptingManager taskExecutionScriptingManager,
            IActionIdentityProvider actionIdentityProvider)
        {
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(azureProxy);
            ArgumentNullException.ThrowIfNull(storageAccessProvider);
            ArgumentNullException.ThrowIfNull(quotaVerifier);
            ArgumentNullException.ThrowIfNull(skuInformationProvider);
            ArgumentNullException.ThrowIfNull(poolFactory);
            ArgumentNullException.ThrowIfNull(taskExecutionScriptingManager);
            ArgumentNullException.ThrowIfNull(actionIdentityProvider);

            this.logger = logger;
            this.azureProxy = azureProxy;
            this.batchPoolManager = batchPoolManager;
            this.storageAccessProvider = storageAccessProvider;
            this.quotaVerifier = quotaVerifier;
            this.skuInformationProvider = skuInformationProvider;

            this.usePreemptibleVmsOnly = batchSchedulingOptions.Value.UsePreemptibleVmsOnly;
            this.batchNodesSubnetId = batchNodesOptions.Value.SubnetId;
            this.batchNodesSetContentMd5OnUpload = batchNodesOptions.Value.ContentMD5;
            this.disableBatchNodesPublicIpAddress = batchNodesOptions.Value.DisablePublicIpAddress;
            this.poolLifetime = TimeSpan.FromDays(batchSchedulingOptions.Value.PoolRotationForcedDays == 0 ? Options.BatchSchedulingOptions.DefaultPoolRotationForcedDays : batchSchedulingOptions.Value.PoolRotationForcedDays);
            this.taskMaxWallClockTime = TimeSpan.FromDays(batchSchedulingOptions.Value.TaskMaxWallClockTimeDays == 0 ? Options.BatchSchedulingOptions.DefaultPoolRotationForcedDays : batchSchedulingOptions.Value.TaskMaxWallClockTimeDays);
            this.defaultStorageAccountName = storageOptions.Value.DefaultAccountName;
            logger.LogDebug(@"Default storage account: {DefaultStorageAccountName}", defaultStorageAccountName);
            this.globalStartTaskPath = StandardizeStartTaskPath(batchNodesOptions.Value.GlobalStartTask, this.defaultStorageAccountName);
            this.globalManagedIdentity = batchNodesOptions.Value.GlobalManagedIdentity;
            this.allowedVmSizesService = allowedVmSizesService;
            this.taskExecutionScriptingManager = taskExecutionScriptingManager;
            this.actionIdentityProvider = actionIdentityProvider;
            batchPoolFactory = poolFactory;
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

            static bool tesTaskIsInitializingOrRunning(TesTask tesTask) => tesTask.State == TesState.INITIALIZING || tesTask.State == TesState.RUNNING;
            static bool tesTaskIsInitializing(TesTask tesTask) => tesTask.State == TesState.INITIALIZING;

            System.Text.Json.JsonSerializerOptions setTaskStateAndLogJsonSerializerOptions = new()
            {
                DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingDefault,
                Converters = { new System.Text.Json.Serialization.JsonStringEnumConverter(System.Text.Json.JsonNamingPolicy.CamelCase) }
            };

            var setTaskStateLock = new object();

            async Task<bool> SetTaskStateAndLogAsync(TesTask tesTask, TesState newTaskState, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
            {
                {
                    var newDataText = SerializeToString(new CombinedBatchTaskInfo(batchInfo, false));

                    if ("{}".Equals(newDataText) && newTaskState == tesTask.State)
                    {
                        logger.LogDebug(@"For task {TesTask} there's nothing to change.", tesTask.Id);
                        return false;
                    }
                    logger.LogDebug(@"Setting task {TesTask} with metadata {BatchTaskState} {Metadata}.", tesTask.Id, batchInfo.State, newDataText);
                }

                var (batchNodeMetrics, taskStartTime, taskEndTime, cromwellRcCode) = newTaskState == TesState.COMPLETE
                    ? await GetBatchNodeMetricsAndCromwellResultCodeAsync(tesTask, cancellationToken)
                    : default;
                var taskAsString = SerializeToString(tesTask);

                lock (setTaskStateLock)
                {
                    tesTask.State = newTaskState;

                    var tesTaskLog = tesTask.GetOrAddTesTaskLog();
                    tesTaskLog.BatchNodeMetrics ??= batchNodeMetrics;
                    tesTaskLog.CromwellResultCode ??= cromwellRcCode;
                    tesTaskLog.EndTime ??= batchInfo.BatchTaskEndTime ?? taskEndTime;
                    tesTaskLog.StartTime ??= batchInfo.BatchTaskStartTime ?? taskStartTime;

                    if (batchInfo.ExecutorEndTime is not null || batchInfo.ExecutorStartTime is not null || batchInfo.ExecutorExitCode is not null)
                    {
                        var tesTaskExecutorLog = tesTaskLog.GetOrAddExecutorLog();
                        tesTaskExecutorLog.StartTime ??= batchInfo.ExecutorStartTime;
                        tesTaskExecutorLog.EndTime ??= batchInfo.ExecutorEndTime;
                        tesTaskExecutorLog.ExitCode ??= batchInfo.ExecutorExitCode;

                        if ((tesTaskExecutorLog?.ExitCode > 1 || tesTaskExecutorLog?.ExitCode < 0) && (tesTaskExecutorLog.Stderr is not null || tesTaskExecutorLog.Stdout is not null))
                        {
                            tesTask.AddToSystemLog(Enumerable.Repeat("Check the following logs:", 1)
                                .Concat(DeserializeJsonStringArray(tesTaskExecutorLog.Stderr) ?? [])
                                .Concat(DeserializeJsonStringArray(tesTaskExecutorLog.Stdout) ?? []));
                        }
                    }

                    if (batchInfo.OutputFileLogs is not null)
                    {
                        tesTaskLog.Outputs ??= [];
                        tesTaskLog.Outputs.AddRange(batchInfo.OutputFileLogs.Select(ConvertOutputFileLogToTesOutputFileLog));
                    }

                    if (batchInfo.Warning is not null)
                    {
                        var warningInfo = batchInfo.Warning.ToList();
                        switch (warningInfo.Count)
                        {
                            case 0:
                                break;
                            case 1:
                                tesTask.SetWarning(warningInfo[0]);
                                break;
                            default:
                                tesTask.SetWarning(warningInfo[0], warningInfo.Skip(1).ToArray());
                                break;
                        }
                    }

                    if (batchInfo.Failure.HasValue)
                    {
                        tesTask.SetFailureReason(
                            batchInfo.Failure.Value.Reason,
                            (batchInfo.Failure.Value.SystemLogs ?? (string.IsNullOrWhiteSpace(batchInfo.AlternateSystemLogItem)
                                    ? []
                                    : Enumerable.Empty<string>().Append(batchInfo.AlternateSystemLogItem))
                                ).ToArray());
                    }
                    else if (!(string.IsNullOrWhiteSpace(batchInfo.AlternateSystemLogItem) || tesTask.IsActiveState() || new[] { TesState.COMPLETE, TesState.CANCELED }.Contains(tesTask.State)))
                    {
                        tesTask.SetFailureReason(batchInfo.AlternateSystemLogItem);
                    }
                }

                return !taskAsString.Equals(SerializeToString(tesTask));

                Tes.Models.TesOutputFileLog ConvertOutputFileLogToTesOutputFileLog(AzureBatchTaskState.OutputFileLog fileLog)
                {
                    return new Tes.Models.TesOutputFileLog
                    {
                        Path = fileLog.Path,
                        SizeBytes = $"{fileLog.Size:D}",
                        Url = fileLog.Url.AbsoluteUri
                    };
                }

                string SerializeToString<T>(T item)
                    => System.Text.Json.JsonSerializer.Serialize(item, setTaskStateAndLogJsonSerializerOptions);

                IEnumerable<string> DeserializeJsonStringArray(string json)
                  => string.IsNullOrWhiteSpace(json) ? null : System.Text.Json.JsonSerializer.Deserialize<IEnumerable<string>>(json, setTaskStateAndLogJsonSerializerOptions);
            }

            async Task<bool> SetCompletedWithErrors(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
            {
                var newTaskState = tesTask.FailureReason switch
                {
                    AzureBatchTaskState.ExecutorError => TesState.EXECUTOR_ERROR,
                    _ => TesState.SYSTEM_ERROR,
                };

                return await SetTaskStateAndLogAsync(tesTask, newTaskState, batchInfo, cancellationToken);
            }

            async Task<bool> TerminateBatchTaskAndSetTaskStateAsync(TesTask tesTask, TesState newTaskState, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken) =>
                await TerminateBatchTaskAsync(tesTask, newTaskState, batchInfo, cancellationToken);

            Task<bool> TerminateBatchTaskAndSetTaskSystemErrorAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken) =>
                TerminateBatchTaskAndSetTaskStateAsync(tesTask, TesState.SYSTEM_ERROR, batchInfo, cancellationToken);

            Task<bool> RequeueTaskAfterFailureAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
                => ++tesTask.ErrorCount > 3
                    ? TerminateBatchTaskAndSetTaskSystemErrorAsync(tesTask, new(batchInfo, "System Error: Retry count exceeded."), cancellationToken)
                    : TerminateBatchTaskAndSetTaskStateAsync(tesTask, TesState.QUEUED, batchInfo, cancellationToken);

            async Task<bool> CancelTaskAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken) =>
                await TerminateBatchTaskAsync(tesTask, TesState.CANCELED, batchInfo, cancellationToken);

            async Task<bool> TerminateBatchTaskAsync(TesTask tesTask, TesState newTaskState, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
            {
                var skipLogInFinally = false;

                try
                {
                    switch (batchInfo.State)
                    {
                        case AzureBatchTaskState.TaskState.CancellationRequested:
                            if (!tesTask.IsActiveState())
                            {
                                skipLogInFinally = true;
                                return await SetTaskStateAndLogAsync(tesTask, newTaskState, batchInfo, cancellationToken);
                            }

                            if (tesTask.Logs?.Any() ?? false)
                            {
                                goto default;
                            }

                            return true; // It was never scheduled

                        default:
                            await azureProxy.TerminateBatchTaskAsync(tesTask.Id, tesTask.PoolId, cancellationToken);
                            return true;
                    }
                }
                catch (BatchException exc) when (BatchErrorCodeStrings.TaskNotFound.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, "Exception terminating batch task with tesTask.Id: {TesTaskId}", tesTask?.Id);
                    throw;
                }
                finally
                {
                    if (!skipLogInFinally)
                    {
                        _ = await SetTaskStateAndLogAsync(tesTask, newTaskState, batchInfo, cancellationToken);
                    }
                }
            }

            bool HandlePreemptedNode(TesTask tesTask, CombinedBatchTaskInfo batchInfo)
            {
                // TODO: Keep track of the number of times Azure Batch retried this task and terminate it as preempted if it is too many times. Are we waiting on Cromwell to support preempted tasks to do this?
                var oldLog = tesTask.GetOrAddTesTaskLog();
                var newLog = tesTask.AddTesTaskLog();
                oldLog.Warning = "ComputeNode was preempted. The task was automatically rescheduled.";
                newLog.VirtualMachineInfo = oldLog.VirtualMachineInfo;
                newLog.StartTime = DateTimeOffset.UtcNow;
                tesTask.State = TesState.INITIALIZING;
                logger.LogInformation("The TesTask {TesTask}'s node was preempted. It was automatically rescheduled.", tesTask.Id);
                return true;
            }

            const string alternateSystemLogMissingFailure = "Please open an issue. There should have been an error reported here.";

            tesTaskStateTransitions =
            [
                new(condition: null, AzureBatchTaskState.TaskState.CancellationRequested, alternateSystemLogItem: null, CancelTaskAsync),
                new(tesTaskIsInitializing, AzureBatchTaskState.TaskState.NodeAllocationFailed, alternateSystemLogItem: null, RequeueTaskAfterFailureAsync),
                new(tesTaskIsInitializing, AzureBatchTaskState.TaskState.NodeStartTaskFailed, alternateSystemLogMissingFailure, TerminateBatchTaskAndSetTaskSystemErrorAsync),
                new(tesTaskIsInitializingOrRunning, AzureBatchTaskState.TaskState.Initializing, alternateSystemLogItem: null, (tesTask, info, ct) => SetTaskStateAndLogAsync(tesTask, TesState.INITIALIZING, info, ct)),
                new(tesTaskIsInitializingOrRunning, AzureBatchTaskState.TaskState.Running, alternateSystemLogItem: null, (tesTask, info, ct) => SetTaskStateAndLogAsync(tesTask, TesState.RUNNING, info, ct)),
                new(tesTaskIsInitializingOrRunning, AzureBatchTaskState.TaskState.CompletedSuccessfully, alternateSystemLogItem: null, (tesTask, info, ct) => SetTaskStateAndLogAsync(tesTask, TesState.COMPLETE, info, ct)),
                new(tesTaskIsInitializingOrRunning, AzureBatchTaskState.TaskState.CompletedWithErrors, alternateSystemLogMissingFailure, SetCompletedWithErrors),
                new(tesTaskIsInitializingOrRunning, AzureBatchTaskState.TaskState.NodeFailedDuringStartupOrExecution, alternateSystemLogMissingFailure, TerminateBatchTaskAndSetTaskSystemErrorAsync),
                new(tesTaskIsInitializingOrRunning, AzureBatchTaskState.TaskState.NodePreempted, alternateSystemLogItem: null, HandlePreemptedNode),
                new(tesTaskIsInitializingOrRunning, AzureBatchTaskState.TaskState.NodeFilesUploadOrDownloadFailed, alternateSystemLogItem: null, (tesTask, info, ct) => SetTaskStateAndLogAsync(tesTask, tesTask.State, info, ct)),
                new(condition: null, AzureBatchTaskState.TaskState.InfoUpdate, alternateSystemLogItem: null, (tesTask, info, ct) => SetTaskStateAndLogAsync(tesTask, tesTask.State, info, ct)),
            ];
        }

        private async Task<bool> DeleteCompletedTaskAsync(string taskId, string jobId, DateTime taskCreated, CancellationToken cancellationToken)
        {
            if (DateTimeOffset.UtcNow <= taskCreated.ToUniversalTime() + BatchDeleteNewTaskWorkaroundTimeSpan)
            {
                return false;
            }

            try
            {
                await azureProxy.DeleteBatchTaskAsync(taskId, jobId, cancellationToken);
                return true;
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.TaskNotFound.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        /// <summary>
        /// Creates a wget command to robustly download a file
        /// </summary>
        /// <param name="urlToDownload">URL to download</param>
        /// <param name="localFilePathDownloadLocation">Filename for the output file</param>
        /// <param name="setExecutable">Whether the file should be made executable or not</param>
        /// <returns>The command to execute</returns>
        internal static string CreateWgetDownloadCommand(Uri urlToDownload, string localFilePathDownloadLocation, bool setExecutable = false)
        {
            ArgumentNullException.ThrowIfNull(urlToDownload);
            ArgumentException.ThrowIfNullOrWhiteSpace(localFilePathDownloadLocation);

            var command = $"wget --no-verbose --https-only --timeout=20 --waitretry=1 --tries=9 --retry-connrefused --continue -O {localFilePathDownloadLocation} '{urlToDownload.AbsoluteUri}'";

            if (setExecutable)
            {
                command += $" && chmod +x {localFilePathDownloadLocation}";
            }

            return command;
        }

        /// <summary>
        /// Retrieves pools associated with this TES from the batch account.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        private IAsyncEnumerable<CloudPool> GetCloudPools(CancellationToken cancellationToken)
            => azureProxy.GetActivePoolsAsync(batchPrefix);

        /// <inheritdoc/>
        public async Task LoadExistingPoolsAsync(CancellationToken cancellationToken)
        {
            await foreach (var cloudPool in GetCloudPools(cancellationToken).WithCancellation(cancellationToken))
            {
                try
                {
                    var forceRemove = !string.IsNullOrWhiteSpace(globalManagedIdentity) && !(cloudPool.Identity?.UserAssignedIdentities?.Any(id => globalManagedIdentity.Equals(id.ResourceId, StringComparison.OrdinalIgnoreCase)) ?? false);
                    await batchPoolFactory().AssignPoolAsync(cloudPool, runnerMD5, forceRemove, cancellationToken);
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, "When retrieving previously created batch pools and jobs, there were one or more failures when trying to access batch pool {PoolId} and/or its associated job.", cloudPool.Id);
                }
            }
        }

        /// <inheritdoc/>
        public async Task UploadTaskRunnerIfNeededAsync(CancellationToken cancellationToken)
        {
            var blobUri = await storageAccessProvider.GetInternalTesBlobUrlAsync(NodeTaskRunnerFilename, storageAccessProvider.BlobPermissionsWithWrite, cancellationToken);
            var blobProperties = await azureProxy.GetBlobPropertiesAsync(blobUri, cancellationToken);
            if (!runnerMD5.Equals(Convert.ToBase64String(blobProperties?.ContentHash ?? []), StringComparison.OrdinalIgnoreCase))
            {
                await azureProxy.UploadBlobFromFileAsync(blobUri, $"scripts/{NodeTaskRunnerFilename}", cancellationToken);
            }
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<RelatedTask<TesTask, bool>> ProcessTesTaskBatchStatesAsync(IEnumerable<TesTask> tesTasks, AzureBatchTaskState[] taskStates, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(tesTasks);
            ArgumentNullException.ThrowIfNull(taskStates);

            return taskStates.Zip(tesTasks, (TaskState, TesTask) => (TaskState, TesTask))
                .Where(entry => entry.TesTask?.IsActiveState() ?? false) // Removes already terminal (and null) TesTasks from being further processed.
                .Select(entry => new RelatedTask<TesTask, bool>(WrapHandleTesTaskTransitionAsync(entry.TesTask, entry.TaskState, cancellationToken), entry.TesTask))
                .WhenEach(cancellationToken, tesTaskTask => tesTaskTask.Task);

            async Task<bool> WrapHandleTesTaskTransitionAsync(TesTask tesTask, AzureBatchTaskState azureBatchTaskState, CancellationToken cancellationToken)
                => await HandleTesTaskTransitionAsync(tesTask, azureBatchTaskState, cancellationToken);
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<RelatedTask<CloudTaskId, bool>> DeleteCloudTasksAsync(IAsyncEnumerable<CloudTaskId> cloudTasks, CancellationToken cancellationToken)
        {
            return cloudTasks.SelectAwaitWithCancellation((task, ct) => ValueTask.FromResult(new RelatedTask<CloudTaskId, bool>(DeleteCompletedTaskAsync(task.TaskId, task.JobId, task.Created, ct), task)));
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

        /// <inheritdoc/>
        public string GetTesTaskIdFromCloudTaskId(string cloudTaskId)
        {
            var separatorIndex = cloudTaskId.LastIndexOf('-');
            return separatorIndex == -1 ? cloudTaskId : cloudTaskId[..separatorIndex];
        }

        // Collections and records managing the processing of TesTasks in Queued status
        private record struct PendingCloudTask(CloudTask CloudTask, TaskCompletionSource TaskCompletion);
        private record struct PendingPoolRequest(string PoolKey, VirtualMachineInformationWithDataDisks VirtualMachineInfo, IList<string> Identities, string PoolDisplayName, TaskCompletionSource<string> TaskCompletion);
        private record struct PendingPool(string PoolKey, VirtualMachineInformationWithDataDisks VirtualMachineInfo, IList<string> Identities, string PoolDisplayName, int InitialTarget, IEnumerable<TaskCompletionSource<string>> TaskCompletions);
        private record struct ImmutableQueueWithTimer<T>(Timer Timer, ImmutableQueue<T> Queue);

        private readonly ConcurrentDictionary<string, ImmutableQueueWithTimer<PendingCloudTask>> _queuedTesTaskPendingTasksByJob = new();
        private readonly ConcurrentDictionary<string, ImmutableQueueWithTimer<PendingPoolRequest>> _queuedTesTaskPendingPoolsByKey = new();
        private readonly ConcurrentQueue<(string JobId, IList<PendingCloudTask> Tasks)> _queuedTesTaskPendingJobBatches = new();
        private readonly ConcurrentQueue<PendingPool> _queuedTesTaskPendingPoolQuotas = new();
        private readonly ConcurrentQueue<PendingPool> _queuedTesTaskPendingPools = new();

        /// <inheritdoc/>
        public async Task<bool> ProcessQueuedTesTaskAsync(TesTask tesTask, CancellationToken cancellationToken)
        {
            string poolKey = default;

            try
            {
                var identities = new List<string>();

                if (!string.IsNullOrWhiteSpace(globalManagedIdentity))
                {
                    identities.Add(globalManagedIdentity);
                }

                if (tesTask.Resources?.ContainsBackendParameterValue(TesResources.SupportedBackendParameters.workflow_execution_identity) == true)
                {
                    var workflowId = tesTask.Resources?.GetBackendParameterValue(TesResources.SupportedBackendParameters.workflow_execution_identity);

                    if (!NodeTaskBuilder.IsValidManagedIdentityResourceId(workflowId))
                    {
                        workflowId = azureProxy.GetManagedIdentityInBatchAccountResourceGroup(workflowId);
                    }

                    identities.Add(workflowId);
                }

                // acrPullIdentity is special. Add it to the end of the list even if it is null, so it is always retrievable.
                identities.Add(await actionIdentityProvider.GetAcrPullActionIdentity(cancellationToken));

                logger.LogDebug(@"Checking quota for {TesTask}.", tesTask.Id);

                var virtualMachineInfo = await GetVmSizeAsync(tesTask, cancellationToken);
                (poolKey, var displayName) = GetPoolKey(tesTask, virtualMachineInfo, identities);
                await quotaVerifier.CheckBatchAccountQuotasAsync(virtualMachineInfo.VM, needPoolOrJobQuotaCheck: !IsPoolAvailable(poolKey), cancellationToken: cancellationToken);

                // double await because the method call returns a System.Task. When that Task returns, the TesTask has been queued to a job and a pool exists to run that job's tasks
                await await AttachQueuedTesTaskToBatchPoolAsync(poolKey, tesTask, virtualMachineInfo, identities, displayName, cancellationToken);

                var tesTaskLog = tesTask.GetOrAddTesTaskLog();
                tesTaskLog.StartTime = DateTimeOffset.UtcNow;
                tesTask.State = TesState.INITIALIZING;
                return true;
            }
            catch (AggregateException aggregateException)
            {
                var result = false;
                var exceptions = new List<Exception>();

                foreach (var partResult in aggregateException.InnerExceptions
                    .Select(ex => QueuedTesTaskHandleExceptionAsync(ex, poolKey, tesTask)))
                {
                    if (partResult.IsFaulted)
                    {
                        exceptions.Add(partResult.Exception);
                    }
                    else
                    {
                        result |= partResult.Result;
                    }
                }

                if (exceptions.Count == 0)
                {
                    return result;
                }
                else
                {
                    throw new AggregateException(exceptions);
                }
            }
            catch (Exception exception)
            {
                var result = QueuedTesTaskHandleExceptionAsync(exception, poolKey, tesTask);

                if (result.IsFaulted)
                {
                    throw result.Exception;
                }
                else
                {
                    return result.Result;
                }
            }
        }

        private async ValueTask<Task> AttachQueuedTesTaskToBatchPoolAsync(string poolKey, TesTask tesTask, VirtualMachineInformationWithDataDisks virtualMachineInfo, IList<string> identities, string poolDisplayName, CancellationToken cancellationToken)
        {
            TaskCompletionSource taskCompletion = new(); // This provides the System.Task this method returns

            try
            {
                var pool = batchPools.TryGetValue(poolKey, out var set) ? set.LastOrDefault(p => p.IsAvailable) : default;

                if (pool is null)
                {
                    TaskCompletionSource<string> poolCompletion = new(); // This provides the poolId of the pool provided for the task
                    AddTValueToCollectorQueue(
                        key: poolKey,
                        value: new PendingPoolRequest(poolKey, virtualMachineInfo, identities, poolDisplayName, poolCompletion),
                        dictionary: _queuedTesTaskPendingPoolsByKey,
                        enqueue: (key, tasks) => _queuedTesTaskPendingPoolQuotas.Enqueue(new(key, tasks.First().VirtualMachineInfo, tasks.First().Identities, tasks.First().PoolDisplayName, tasks.Count, tasks.Select(t => t.TaskCompletion))),
                        groupGatherWindow: QueuedTesTaskPoolGroupGatherWindow,
                        maxCount: int.MaxValue);

                    pool = batchPools.GetPoolOrDefault(await poolCompletion.Task); // This ensures that the pool is managed by this BatchScheduler

                    if (pool is null)
                    {
                        throw new System.Diagnostics.UnreachableException("Pool should have been obtained by this point.");
                    }
                }

                var tesTaskLog = tesTask.AddTesTaskLog();
                tesTaskLog.VirtualMachineInfo = virtualMachineInfo.VM;
                var cloudTaskId = $"{tesTask.Id}-{tesTask.Logs.Count}";
                tesTask.PoolId = pool.PoolId;
                var cloudTask = await ConvertTesTaskToBatchTaskUsingRunnerAsync(cloudTaskId, tesTask, identities.Last(), virtualMachineInfo.VM.VmFamily, cancellationToken);

                logger.LogInformation(@"Creating batch task for TES task {TesTaskId}. Using VM size {VmSize}.", tesTask.Id, virtualMachineInfo.VM.VmSize);

                AddTValueToCollectorQueue(
                    key: pool.PoolId,
                    value: new(cloudTask, taskCompletion),
                    dictionary: _queuedTesTaskPendingTasksByJob,
                    enqueue: (key, tasks) => _queuedTesTaskPendingJobBatches.Enqueue((key, tasks)),
                    groupGatherWindow: QueuedTesTaskTaskGroupGatherWindow,
                    maxCount: 100);
            }
            catch (Exception exception)
            {
                taskCompletion.SetException(exception);
            }

            return taskCompletion.Task;
        }

        /// <summary>
        /// Adds an entry to the queue in the directory's value for a key in a IDisposable-safe pattern and set that timer after the entry is in the dictionary only once.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <param name="dictionary">The grouping dictionary into which to insert <paramref name="value"/> into the <paramref name="key"/>'s value's queue.</param>
        /// <param name="enqueue">The method to transfer the group to the processing queue.</param>
        /// <param name="groupGatherWindow">The time period within which any additional tasks are added to the same group.</param>
        /// <param name="maxCount">The maximum group size.</param>
        private static void AddTValueToCollectorQueue<TKey, TValue>(TKey key, TValue value, ConcurrentDictionary<TKey, ImmutableQueueWithTimer<TValue>> dictionary, Action<TKey, IList<TValue>> enqueue, TimeSpan groupGatherWindow, int maxCount)
        {
            // Save a list of timers created in this method invocation because ConcurrentDictionary.AddOrUpdate() can call addValueFactory any number of times and never store the result anywhere, resulting in created timers without reachable references
            List<Timer> timers = [];
            var entry = dictionary.AddOrUpdate(key: key,
                addValueFactory: key =>
                    new(Timer: CreateTimer(
                        callback: state => QueuedTesTaskAddTaskEntryToQueueFromDirectory(
                            key: (TKey)state,
                            dictionary: dictionary,
                            enqueue: enqueue,
                            groupGatherWindow: groupGatherWindow,
                            maxCount: maxCount),
                        state: key),
                    Queue: [value]),
                updateValueFactory: (key, entry) => new(Timer: entry.Timer, Queue: entry.Queue.Enqueue(value)));

            // If a new entry in the dictionary was created, set that entry's timer to run and don't dispose that timer
            if (timers.Remove(entry.Timer))
            {
                entry.Timer.Change(groupGatherWindow, Timeout.InfiniteTimeSpan);
            }

            // Dispose all remaining timers
            timers.ForEach(t => t.Dispose());

            Timer CreateTimer(TimerCallback callback, TKey state)
            {
                Timer timer = new(callback, state, Timeout.Infinite, Timeout.Infinite);
                timers.Add(timer);
                return timer;
            }
        }

        // Move entries from a ConcurrentDictionary entry (collection queue) to another queue (processing queue) as a single entry
        private static void QueuedTesTaskAddTaskEntryToQueueFromDirectory<TKey, TValue>(TKey key, ConcurrentDictionary<TKey, ImmutableQueueWithTimer<TValue>> dictionary, Action<TKey, IList<TValue>> enqueue, TimeSpan groupGatherWindow, int maxCount)
        {
            if (!dictionary.TryGetValue(key, out var refValue))
            {
                return; // Quick return
            }

            var (timer, queue) = refValue;
            List<TValue> tasks = [];

            while (!queue.IsEmpty && tasks.Count < maxCount)
            {
                queue = queue.Dequeue(out var task);
                tasks.Add(task);
            }

            enqueue(key, tasks);

            // Remove enqueued entries from directory without leaving empty entries. This is a loop because we are using ConcurrentDirectory
            for (;
                !(queue.IsEmpty switch
                {
                    true => dictionary.TryRemove(new(key, refValue)),
                    false => dictionary.TryUpdate(key, new(timer, queue), refValue),
                });
                queue = ImmutableQueue.CreateRange(refValue.Queue.WhereNot(tasks.Contains)))
            {
                refValue = dictionary[key];
            }

            if (queue.IsEmpty)
            {
                // Entry was removed from directory
                timer.Dispose();
            }
            else
            {
                // Entry was retained in directory
                timer.Change(groupGatherWindow, Timeout.InfiniteTimeSpan);
            }
        }

        /// <inheritdoc/>
        public async ValueTask PerformBackgroundTasksAsync(CancellationToken cancellationToken)
        {
            // Add a batch of tasks to a job
            if (_queuedTesTaskPendingJobBatches.TryDequeue(out var jobBatch))
            {
                var (jobId, tasks) = jobBatch;
                logger.LogDebug(@"Adding {AddedTasks} tasks to {CloudJob}.", tasks.Count, jobId);
                await PerformTaskAsync(
                    method: async token => await azureProxy.AddBatchTasksAsync(tasks.Select(t => t.CloudTask), jobId, token),
                    taskCompletions: tasks.Select(task => task.TaskCompletion),
                    cancellationToken: cancellationToken);
            }

            // Apply Pool and Job Quota limits
            {
                Dictionary<string, PendingPool> pools = [];

                while (_queuedTesTaskPendingPoolQuotas.TryDequeue(out var pendingPool))
                {
                    pools.Add(pendingPool.PoolKey, pendingPool);
                }

                if (pools.Count != 0)
                {
                    // Determine how many new pools/jobs we need now
                    var requiredNewPools = pools.Keys.WhereNot(IsPoolAvailable).ToList();

                    // Revisit pool/job quotas (the task quota analysis already dealt with the possibility of needing just one more pool or job).
                    if (requiredNewPools.Skip(1).Any())
                    {
                        // This will remove pool keys we cannot accommodate due to quota, along with all of their associated tasks, from being queued into Batch.
                        logger.LogDebug(@"Checking pools and jobs quota to accommodate {NeededPools} additional pools.", requiredNewPools.Count);

                        var (exceededQuantity, exception) = await quotaVerifier.CheckBatchAccountPoolAndJobQuotasAsync(requiredNewPools.Count, cancellationToken);

                        foreach (var task in ((IEnumerable<string>)requiredNewPools)
                            .Reverse()
                            .SelectWhere<string, PendingPool>(TryRemovePool)
                            .Take(exceededQuantity)
                            .SelectMany(t => t.TaskCompletions))
                        {
                            task.SetException(exception);
                        }

                        bool TryRemovePool(string key, out PendingPool result)
                        {
                            logger.LogDebug(@"Due to quotas, unable to accommodate {PoolKey} batch pools.", key);
                            result = pools[key];
                            pools.Remove(key);
                            return true;
                        }
                    }

                    logger.LogDebug(@"Obtaining {NewPools} batch pools.", pools.Count);

                    foreach (var poolToCreate in pools)
                    {
                        _queuedTesTaskPendingPools.Enqueue(poolToCreate.Value);
                    }
                }
            }

            // Create a batch pool
            if (_queuedTesTaskPendingPools.TryDequeue(out var pool))
            {
                logger.LogDebug(@"Creating pool for {PoolKey}.", pool.PoolKey);
                var useGen2 = pool.VirtualMachineInfo.VM.HyperVGenerations?.Contains("V2", StringComparer.OrdinalIgnoreCase);
                await PerformTaskOfTAsync(
                    method: async token => (await GetOrAddPoolAsync(
                            key: pool.PoolKey,
                            isPreemptable: pool.VirtualMachineInfo.VM.LowPriority,
                            modelPoolFactory: async (id, ct) => await GetPoolSpecification(
                                name: id,
                                displayName: pool.PoolDisplayName,
                                poolIdentity: GetBatchPoolIdentity(pool.Identities.WhereNot(string.IsNullOrWhiteSpace).ToList()),
                                vmInfo: pool.VirtualMachineInfo,
                                initialTarget: pool.InitialTarget,
                                nodeInfo: (useGen2 ?? false) ? gen2BatchNodeInfo : gen1BatchNodeInfo,
                                cancellationToken: ct),
                            cancellationToken: token))
                        .PoolId,
                    taskCompletions: pool.TaskCompletions,
                    cancellationToken: cancellationToken);
            }

            async static ValueTask PerformTaskAsync(Func<CancellationToken, ValueTask> method, IEnumerable<TaskCompletionSource> taskCompletions, CancellationToken cancellationToken)
            {
                try
                {
                    await method(cancellationToken);
                    taskCompletions.ForEach(completion => completion.SetResult());
                }
                catch (Exception exception)
                {
                    taskCompletions.ForEach(completion => completion.SetException(new AggregateException(Enumerable.Empty<Exception>().Append(exception))));
                }
            }

            async static ValueTask PerformTaskOfTAsync<T>(Func<CancellationToken, ValueTask<T>> method, IEnumerable<TaskCompletionSource<T>> taskCompletions, CancellationToken cancellationToken)
            {
                try
                {
                    var result = await method(cancellationToken);
                    taskCompletions.ForEach(completion => completion.SetResult(result));
                }
                catch (Exception exception)
                {
                    taskCompletions.ForEach(completion => completion.SetException(new AggregateException(Enumerable.Empty<Exception>().Append(exception))));
                }
            }
        }

        Task<bool> QueuedTesTaskHandleExceptionAsync(Exception exception, string poolKey, TesTask tesTask)
        {
            switch (exception)
            {
                case AzureBatchPoolCreationException azureBatchPoolCreationException:
                    if (!azureBatchPoolCreationException.IsTimeout && !azureBatchPoolCreationException.IsJobQuota && !azureBatchPoolCreationException.IsPoolQuota && azureBatchPoolCreationException.InnerException is not null)
                    {
                        return QueuedTesTaskHandleExceptionAsync(azureBatchPoolCreationException.InnerException, poolKey, tesTask);
                    }

                    logger.LogWarning(azureBatchPoolCreationException, "TES task: {TesTask} AzureBatchPoolCreationException.Message: {ExceptionMessage}. This might be a transient issue. Task will remain with state QUEUED. Confirmed timeout: {ConfirmedTimeout}", tesTask.Id, azureBatchPoolCreationException.Message, azureBatchPoolCreationException.IsTimeout);

                    if (azureBatchPoolCreationException.IsJobQuota || azureBatchPoolCreationException.IsPoolQuota)
                    {
                        neededPools.Add(poolKey);
                        tesTask.SetWarning(azureBatchPoolCreationException.InnerException switch
                        {
                            null => "Unknown reason",
                            Azure.RequestFailedException requestFailedException => $"{requestFailedException.ErrorCode}: \"{requestFailedException.Message}\"{requestFailedException.Data.Keys.Cast<string>().Zip(requestFailedException.Data.Values.Cast<string>()).Select(p => $"\n{p.First}: {p.Second}")}",
                            Microsoft.Rest.Azure.CloudException cloudException => cloudException.Body.Message,
                            var e when e is BatchException batchException && batchException.InnerException is Microsoft.Azure.Batch.Protocol.Models.BatchErrorException batchErrorException => batchErrorException.Body.Message.Value,
                            _ => "Unknown reason",
                        });
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
                    tesTask.SetWarning(batchErrorException.Body.Message.Value, []);
                    logger.LogInformation("Not enough job quota available for task Id {TesTask}. Reason: {BodyMessage}. Task will remain in queue.", tesTask.Id, batchErrorException.Body.Message.Value);
                    break;

                case BatchException batchException when batchException.InnerException is Microsoft.Azure.Batch.Protocol.Models.BatchErrorException batchErrorException && AzureBatchPoolCreationException.IsPoolQuotaException(batchErrorException.Body.Code):
                    neededPools.Add(poolKey);
                    tesTask.SetWarning(batchErrorException.Body.Message.Value, []);
                    logger.LogInformation("Not enough pool quota available for task Id {TesTask}. Reason: {BodyMessage}. Task will remain in queue.", tesTask.Id, batchErrorException.Body.Message.Value);
                    break;

                case Microsoft.Rest.Azure.CloudException cloudException when AzureBatchPoolCreationException.IsPoolQuotaException(cloudException.Body.Code):
                    neededPools.Add(poolKey);
                    tesTask.SetWarning(cloudException.Body.Message, []);
                    logger.LogInformation("Not enough pool quota available for task Id {TesTask}. Reason: {BodyMessage}. Task will remain in queue.", tesTask.Id, cloudException.Body.Message);
                    break;

                default:
                    tesTask.State = TesState.SYSTEM_ERROR;
                    tesTask.SetFailureReason(AzureBatchTaskState.UnknownError, $"{exception?.GetType().FullName}: {exception?.Message}", exception?.StackTrace);
                    logger.LogError(exception, "TES task: {TesTask} Exception: {ExceptionType}: {ExceptionMessage}", tesTask.Id, exception?.GetType().FullName, exception?.Message);
                    break;
            }

            return Task.FromResult(true);
        }

        /// <summary>
        /// Transitions the <see cref="TesTask"/> to the new state, based on the rules defined in the tesTaskStateTransitions list.
        /// </summary>
        /// <param name="tesTask">TES task</param>
        /// <param name="azureBatchTaskState">Current Azure Batch task info</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>True if the TES task was changed.</returns>
        private ValueTask<bool> HandleTesTaskTransitionAsync(TesTask tesTask, AzureBatchTaskState azureBatchTaskState, CancellationToken cancellationToken)
            => tesTaskStateTransitions
                .FirstOrDefault(m => (m.Condition is null || m.Condition(tesTask)) && (m.CurrentBatchTaskState is null || m.CurrentBatchTaskState == azureBatchTaskState.State))
                .ActionAsync(tesTask, azureBatchTaskState, cancellationToken);

        private async Task<CloudTask> ConvertTesTaskToBatchTaskUsingRunnerAsync(string taskId, TesTask task, string acrPullIdentity, string vmFamily,
            CancellationToken cancellationToken)
        {
            var nodeTaskCreationOptions = await GetNodeTaskConversionOptionsAsync(task, acrPullIdentity, GetVmFamily(vmFamily), cancellationToken);

            var assets = await taskExecutionScriptingManager.PrepareBatchScriptAsync(task, nodeTaskCreationOptions, cancellationToken);

            var batchRunCommand = taskExecutionScriptingManager.ParseBatchRunCommand(assets);

            return new(taskId, batchRunCommand)
            {
                Constraints = new(maxWallClockTime: taskMaxWallClockTime, retentionTime: TimeSpan.Zero, maxTaskRetryCount: 0),
                UserIdentity = new(new AutoUserSpecification(elevationLevel: ElevationLevel.Admin, scope: AutoUserScope.Pool)),
                EnvironmentSettings = assets.Environment?.Select(pair => new EnvironmentSetting(pair.Key, pair.Value)).ToList(),
            };
        }

        private async Task<NodeTaskConversionOptions> GetNodeTaskConversionOptionsAsync(TesTask task, string acrPullIdentity, VmFamilySeries vmFamily, CancellationToken cancellationToken)
        {
            return new(
                DefaultStorageAccountName: defaultStorageAccountName,
                AdditionalInputs: await GetAdditionalCromwellInputsAsync(task, cancellationToken),
                GlobalManagedIdentity: globalManagedIdentity,
                AcrPullIdentity: acrPullIdentity,
                DrsHubApiHost: drsHubApiHost,
                VmFamilyGroup: vmFamily,
                SetContentMd5OnUpload: batchNodesSetContentMd5OnUpload
            );
        }

        private async ValueTask<IList<TesInput>> GetAdditionalCromwellInputsAsync(TesTask task, CancellationToken cancellationToken)
        {
            // TODO: Cromwell bug: Cromwell command write_tsv() generates a file in the execution directory, for example execution/write_tsv_3922310b441805fc43d52f293623efbc.tmp. These are not passed on to TES inputs.
            // WORKAROUND: Get the list of files in the execution directory and add them to task inputs.
            return task.IsCromwell()
                ? await GetExistingBlobsInCromwellStorageLocationAsTesInputsAsync(task, cancellationToken) ?? []
                : [];
        }

        private async ValueTask<List<TesInput>> GetExistingBlobsInCromwellStorageLocationAsTesInputsAsync(TesTask task, CancellationToken cancellationToken)
        {
            var metadata = task.GetCromwellMetadata();

            var cromwellExecutionDirectory = (Uri.TryCreate(metadata.CromwellRcUri, UriKind.Absolute, out var uri) && !uri.IsFile)
                ? GetParentUrl(metadata.CromwellRcUri)
                : $"/{GetParentUrl(metadata.CromwellRcUri)}";

            var executionDirectoryUri = await storageAccessProvider.MapLocalPathToSasUrlAsync(cromwellExecutionDirectory,
                BlobSasPermissions.List, cancellationToken);
            var commandScript =
                task.Inputs?.FirstOrDefault(b => "commandScript".Equals(b.Name));

            if (executionDirectoryUri is not null && commandScript is not null)
            {
                var executionDirectoryBlobName = new Azure.Storage.Blobs.BlobUriBuilder(executionDirectoryUri).BlobName;
                var pathBlobPrefix = commandScript.Path[..commandScript.Path.IndexOf(executionDirectoryBlobName, StringComparison.OrdinalIgnoreCase)];

                var blobsInExecutionDirectory =
                    await azureProxy.ListBlobsAsync(executionDirectoryUri, cancellationToken).ToListAsync(cancellationToken);
                var scriptBlob =
                    blobsInExecutionDirectory.FirstOrDefault(b => commandScript.Path.Equals($"{pathBlobPrefix}/{b.BlobName}", StringComparison.Ordinal));

                if (default != scriptBlob)
                {
                    blobsInExecutionDirectory.Remove(scriptBlob);
                }

                if (commandScript is not null)
                {
                    return blobsInExecutionDirectory
                        .Select(b => (Path: $"/{metadata.CromwellExecutionDir.TrimStart('/')}/{b.BlobName.Split('/').Last()}",
                            Uri: new BlobUriBuilder(executionDirectoryUri) { BlobName = b.BlobName }.ToUri()))
                        .Select(b => new TesInput
                        {
                            Path = b.Path,
                            Url = b.Uri.AbsoluteUri,
                            Name = Path.GetFileName(b.Path),
                            Type = TesFileType.FILE
                        })
                        .ToList();
                }
            }

            return default;
        }

        internal static VmFamilySeries GetVmFamily(string vmFamily)
        {
            return _vmFamilyParseData.Keys.FirstOrDefault(key => VmFamilyMatches(key, vmFamily.Replace(" ", string.Empty)));

            static bool VmFamilyMatches(VmFamilySeries startScriptVmFamily, string vmFamily)
            {
                var (prefixes, suffixes) = _vmFamilyParseData[startScriptVmFamily];
                return prefixes.Any(prefix => vmFamily.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                    && suffixes.Any(suffix => vmFamily.EndsWith(suffix, StringComparison.OrdinalIgnoreCase));
            }
        }

        private static readonly IReadOnlyDictionary<VmFamilySeries, (IEnumerable<string> Prefixes, IEnumerable<string> Suffixes)> _vmFamilyParseData = new Dictionary<VmFamilySeries, (IEnumerable<string> Prefixes, IEnumerable<string> Suffixes)>()
        {
            // Order is important. Put "families" after specific family entries, as these are matched in order.
            //{ VmFamilySeries.standardLSFamily, new(["standardLS", "standardLAS"], ["Family"]) },
            { VmFamilySeries.standardNPSFamily, new(["standardNPS"], ["Family"]) },
            { VmFamilySeries.standardNCFamilies, new(["standardNC"], ["Family"]) },
            { VmFamilySeries.standardNDFamilies, new(["standardND"], ["Family"]) },
            { VmFamilySeries.standardNVFamilies, new(["standardNV"], ["v3Family"/*, "v4Family", "v5Family"*/]) }, // Currently, NVv4 and NVv5 do not work
        }.AsReadOnly();

        /// <summary>
        /// Selected vmFamily groups
        /// </summary>
        public enum /*StartScript*/VmFamilySeries
        {
            /// <summary>
            /// Standard NPS family
            /// </summary>
            /// <remarks>FPGA, not GPU.</remarks>
            standardNPSFamily,

            /// <summary>
            /// Standard NC families
            /// </summary>
            standardNCFamilies,

            /// <summary>
            /// Standard ND families
            /// </summary>
            standardNDFamilies,

            /// <summary>
            /// Standard NG families
            /// </summary>
            /// <remarks>AMD GPUs, only Windows drivers are available.</remarks>
            standardNGFamilies,

            /// <summary>
            /// Standard NV families
            /// </summary>
            standardNVFamilies,
        }

        private static NodeOS GetNodeOS(BatchModels.BatchVmConfiguration vmConfig)
            => vmConfig.NodeAgentSkuId switch
            {
                var s when s.StartsWith("batch.node.ubuntu ", StringComparison.OrdinalIgnoreCase) => NodeOS.Ubuntu,
                var s when s.StartsWith("batch.node.centos ", StringComparison.OrdinalIgnoreCase) => NodeOS.Centos,
                _ => throw new InvalidOperationException($"Unrecognized OS. Please send open an issue @ 'https://github.com/microsoft/ga4gh-tes/issues' with this message ({vmConfig.NodeAgentSkuId})")
            };

        private enum NodeOS
        {
            Ubuntu,
            Centos,
        }

        /// <summary>
        /// Constructs a universal Azure Start Task instance
        /// </summary>
        /// <param name="poolId">Pool Id</param>
        /// <param name="machineConfiguration">A <see cref="BatchModels.BatchVmConfiguration"/> describing the OS of the pool's nodes.</param>
        /// <param name="vmInfo"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        /// <remarks>This method also mitigates errors associated with docker daemons that are not configured to place their filesystem assets on the data drive.</remarks>
        private async Task<BatchModels.BatchAccountPoolStartTask> GetStartTaskAsync(string poolId, BatchModels.BatchVmConfiguration machineConfiguration, VirtualMachineInformationWithDataDisks vmInfo, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(poolId);
            ArgumentNullException.ThrowIfNull(machineConfiguration);
            ArgumentNullException.ThrowIfNull(vmInfo);
            ArgumentNullException.ThrowIfNull(vmInfo.DataDisks, nameof(vmInfo));
            ArgumentNullException.ThrowIfNull(vmInfo.VM, nameof(vmInfo));

            List<BatchModels.BatchVmExtension> extensions = [];

            var nodeOs = GetNodeOS(machineConfiguration);
            var vmFamilySeries = GetVmFamily(vmInfo.VM.VmFamily);
            var vmGen = (vmInfo.VM.HyperVGenerations ?? []).OrderBy(g => g, StringComparer.OrdinalIgnoreCase).LastOrDefault() ?? "<missing>";

            var globalStartTaskConfigured = !string.IsNullOrWhiteSpace(globalStartTaskPath);

            var globalStartTaskSasUrl = globalStartTaskConfigured
                ? await storageAccessProvider.MapLocalPathToSasUrlAsync(globalStartTaskPath, BlobSasPermissions.Read, cancellationToken, sasTokenDuration: PoolScheduler.RunInterval.Multiply(2).Add(poolLifetime).Add(TimeSpan.FromMinutes(15)))
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
            cmd.Append($"mkdir -p {BatchNodeSharedEnvVar} && {DownloadViaWget(await storageAccessProvider.GetInternalTesBlobUrlAsync(NodeTaskRunnerFilename, BlobSasPermissions.Read, cancellationToken), $"{BatchNodeSharedEnvVar}/{NodeTaskRunnerFilename}", setExecutable: true)}");

            if (!dockerConfigured)
            {
                var packageInstallScript = nodeOs switch
                {
                    NodeOS.Ubuntu => "echo \"Ubuntu OS detected\"",
                    NodeOS.Centos => "sudo yum install epel-release -y && sudo yum update -y && sudo yum install -y wget",
                    _ => throw new InvalidOperationException($"Unrecognized OS. Please send open an issue @ 'https://github.com/microsoft/ga4gh-tes/issues' with this message ({machineConfiguration.NodeAgentSkuId})")
                };

                var script = "config-docker.sh";
                // TODO: optimize this by uploading all vmfamily scripts when uploading runner binary rather then for each individual pool as relevant
                cmd.Append($" && {DownloadAndExecuteScriptAsync(await UploadScriptAsync(script, await ReadScriptAsync("config-docker.sh", sb => sb.Replace("{PackageInstalls}", packageInstallScript))), $"{BatchNodeTaskWorkingDirEnvVar}/{script}")}");
            }

            string vmFamilyStartupScript = null;

            if (vmInfo.DataDisks.Count > 0)
            {
                vmFamilyStartupScript = @"config-disks.sh";
            }
            else if ((vmInfo.VM.NvmeDiskSizeInGiB ?? 0) > 0)
            {
                vmFamilyStartupScript = @"config-nvme.sh";
            }

            if (!string.IsNullOrWhiteSpace(vmFamilyStartupScript))
            {
                var script = "config-vmfamily.sh";
                // TODO: optimize this by uploading all vmfamily scripts when uploading runner binary rather then for each individual pool as relevant
                cmd.Append($" && {DownloadAndExecuteScriptAsync(await UploadScriptAsync(script, await ReadScriptAsync(vmFamilyStartupScript, sb => sb.Replace("{HctlPrefix}", vmGen switch { "V1" => "3:0:0", "V2" => "1:0:0", _ => throw new InvalidOperationException($"Unrecognized VM Generation. Please send open an issue @ 'https://github.com/microsoft/ga4gh-tes/issues' with this message ({vmGen})") }))), $"{BatchNodeTaskWorkingDirEnvVar}/{script}")}");
            }

            switch (vmFamilySeries)
            {
                case VmFamilySeries.standardNCFamilies:
                case VmFamilySeries.standardNDFamilies:
                case VmFamilySeries.standardNVFamilies:
                    if (vmInfo.VM.GpusAvailable > 0)
                    {
                        // https://learn.microsoft.com/en-us/azure/virtual-machines/extensions/hpccompute-gpu-linux
                        extensions.Add(new(name: "gpu", publisher: "Microsoft.HpcCompute", extensionType: "NvidiaGpuDriverLinux") { AutoUpgradeMinorVersion = true, TypeHandlerVersion = "1.6" });
                        var script = nodeOs switch
                        {
                            NodeOS.Ubuntu => @"config-n-gpu-apt.sh",
                            NodeOS.Centos => @"config-n-gpu-yum.sh",
                            _ => throw new InvalidOperationException($"Unrecognized OS. Please send open an issue @ 'https://github.com/microsoft/ga4gh-tes/issues' with this message ({machineConfiguration.NodeAgentSkuId})"),
                        };
                        // TODO: optimize this by uploading all non-personalized scripts when uploading runner binary rather then for each individual pool
                        cmd.Append($" && {DownloadAndExecuteScriptAsync(await UploadScriptAsync(script, await ReadScriptAsync(script)), $"{BatchNodeTaskWorkingDirEnvVar}/{script}")}");
                    }
                    break;
            }

            if (globalStartTaskConfigured)
            {
                cmd.Append($" && {DownloadAndExecuteScriptAsync(globalStartTaskSasUrl, $"{BatchNodeTaskWorkingDirEnvVar}/global-{StartTaskScriptFilename}")}");
            }

            machineConfiguration.Extensions.AddRange(extensions);

            return new()
            {
                CommandLine = $"/bin/sh -c \"{DownloadViaWget(await UploadScriptAsync(StartTaskScriptFilename, cmd), $"{BatchNodeTaskWorkingDirEnvVar}/{StartTaskScriptFilename}", execute: true)}\"",
                UserIdentity = new() { AutoUser = new() { ElevationLevel = BatchModels.BatchUserAccountElevationLevel.Admin, Scope = BatchModels.BatchAutoUserScope.Pool } },
                MaxTaskRetryCount = 4,
                WaitForSuccess = true
            };

            string DownloadAndExecuteScriptAsync(Uri url, string localFilePathDownloadLocation) // TODO: download via node runner
                => DownloadViaWget(url, localFilePathDownloadLocation, execute: true);

            string DownloadViaWget(Uri url, string localFilePathDownloadLocation, bool execute = false, bool setExecutable = false)
            {
                var content = CreateWgetDownloadCommand(url, localFilePathDownloadLocation, setExecutable: execute || setExecutable);
                return execute
                    ? $"{content} && {localFilePathDownloadLocation}"
                    : content;
            }

            async ValueTask<Uri> UploadScriptAsync(string name, StringBuilder content)
            {
                content.AppendLinuxLine(string.Empty);
                var path = $"/pools/{poolId}/{name}";
                await azureProxy.UploadBlobAsync(await storageAccessProvider.GetInternalTesBlobUrlAsync(path, BlobSasPermissions.Write, cancellationToken), content.ToString(), cancellationToken);
                content.Clear();
                return await storageAccessProvider.GetInternalTesBlobUrlAsync(path, BlobSasPermissions.Read, cancellationToken);
            }

            async ValueTask<StringBuilder> ReadScriptAsync(string name, Action<StringBuilder> munge = default)
            {
                var path = Path.Combine(AppContext.BaseDirectory, "scripts", name);
                StringBuilder content = new((await File.ReadAllTextAsync(path, cancellationToken))
                    .ReplaceLineEndings("\n"));
                munge?.Invoke(content);
                return content;
            }
        }

        /// <summary>
        /// Generate the BatchPoolIdentity object
        /// </summary>
        /// <param name="identities"></param>
        /// <returns></returns>
        private static Azure.ResourceManager.Models.ManagedServiceIdentity GetBatchPoolIdentity(IList<string> identities)
        {
            if (identities is null || identities.Count == 0)
            {
                return null;
            }

            Azure.ResourceManager.Models.ManagedServiceIdentity result = new(Azure.ResourceManager.Models.ManagedServiceIdentityType.UserAssigned);
            result.UserAssignedIdentities.AddRange(identities.ToDictionary(identity => new Azure.Core.ResourceIdentifier(identity), _ => new Azure.ResourceManager.Models.UserAssignedIdentity()));
            return result;
        }

        /// <summary>
        /// Generate the <see cref="BatchAccountPoolData"/> for the needed pool.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="displayName"></param>
        /// <param name="poolIdentity"></param>
        /// <param name="vmInfo"></param>
        /// <param name="initialTarget"></param>
        /// <param name="nodeInfo"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>The specification for the pool <see cref="BatchAccountPoolData"/>.</returns>
        /// <remarks>
        /// Devs: Any changes to any properties set in this method will require corresponding changes to all classes implementing <see cref="Management.Batch.IBatchPoolManager"/> along with possibly any systems they call, with the possible exception of <seealso cref="Management.Batch.ArmBatchPoolManager"/>.
        /// </remarks>
        private async ValueTask<BatchAccountPoolData> GetPoolSpecification(string name, string displayName, Azure.ResourceManager.Models.ManagedServiceIdentity poolIdentity, VirtualMachineInformationWithDataDisks vmInfo, int initialTarget, BatchNodeInfo nodeInfo, CancellationToken cancellationToken)
        {
            ValidateString(name, 64);
            ValidateString(displayName, 1024);

            var vmConfig = new BatchModels.BatchVmConfiguration(
                imageReference: new()
                {
                    Publisher = nodeInfo.BatchImagePublisher,
                    Offer = nodeInfo.BatchImageOffer,
                    Sku = nodeInfo.BatchImageSku,
                    Version = nodeInfo.BatchImageVersion,
                },
                nodeAgentSkuId: nodeInfo.BatchNodeAgentSkuId);

            if (vmInfo.VM.EncryptionAtHostSupported ?? false)
            {
                vmConfig.DiskEncryptionTargets.AddRange([BatchModels.BatchDiskEncryptionTarget.OSDisk, BatchModels.BatchDiskEncryptionTarget.TemporaryDisk]);
            }

            vmConfig.DataDisks.AddRange(vmInfo.DataDisks.Select(VirtualMachineInformationWithDataDisks.ToBatchVmDataDisk));

            BatchAccountPoolData poolSpecification = new()
            {
                DisplayName = displayName,
                Identity = poolIdentity,
                VmSize = vmInfo.VM.VmSize,
                ScaleSettings = new() { AutoScale = new(BatchPool.AutoPoolFormula(vmInfo.VM.LowPriority, initialTarget)) { EvaluationInterval = BatchPool.AutoScaleEvaluationInterval } },
                DeploymentVmConfiguration = vmConfig,
                //ApplicationPackages = ,
                StartTask = await GetStartTaskAsync(name, vmConfig, vmInfo, cancellationToken),
                TargetNodeCommunicationMode = BatchModels.NodeCommunicationMode.Simplified,
            };

            poolSpecification.Metadata.Add(new(string.Empty, name));

            if (!string.IsNullOrEmpty(batchNodesSubnetId))
            {
                poolSpecification.NetworkConfiguration = new()
                {
                    PublicIPAddressConfiguration = new() { Provision = disableBatchNodesPublicIpAddress ? BatchModels.BatchIPAddressProvisioningType.NoPublicIPAddresses : BatchModels.BatchIPAddressProvisioningType.BatchManaged },
                    SubnetId = new(batchNodesSubnetId),
                };
            }

            return poolSpecification;

            static void ValidateString(string value, int maxLength, [System.Runtime.CompilerServices.CallerArgumentExpression(nameof(value))] string paramName = null)
            {
                ArgumentNullException.ThrowIfNull(value, paramName);

                if (value.Length > maxLength) throw new ArgumentException($"{paramName} exceeds maximum length {maxLength}", paramName);
            }
        }

        /// <summary>
        /// Gets the cheapest available VM size that satisfies the <see cref="TesTask"/> execution requirements
        /// </summary>
        /// <param name="tesTask"><see cref="TesTask"/></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="forcePreemptibleVmsOnly">Force consideration of preemptible virtual machines only.</param>
        /// <returns>The virtual machine info</returns>
        internal async Task<VirtualMachineInformationWithDataDisks> GetVmSizeAsync(TesTask tesTask, CancellationToken cancellationToken, bool forcePreemptibleVmsOnly = false)
        {
            var allowedVmSizes = await allowedVmSizesService.GetAllowedVmSizes(cancellationToken);
            bool AllowedVmSizesFilter(VirtualMachineInformationWithDataDisks vm) => allowedVmSizes is null || !allowedVmSizes.Any() || allowedVmSizes.Contains(vm.VM.VmSize, StringComparer.OrdinalIgnoreCase) || allowedVmSizes.Contains(vm.VM.VmFamily, StringComparer.OrdinalIgnoreCase);

            var tesResources = tesTask.Resources;

            var previouslyFailedVmSizes = tesTask.Logs?
                .Where(log => log.FailureReason == AzureBatchTaskState.TaskState.NodeAllocationFailed.ToString() && log.VirtualMachineInfo?.VmSize is not null)
                .Select(log => log.VirtualMachineInfo.VmSize)
                .Distinct()
                .ToList();

            var virtualMachineInfoList = await skuInformationProvider.GetVmSizesAndPricesAsync(azureProxy.GetArmRegion(), cancellationToken);
            var preemptible = forcePreemptibleVmsOnly || usePreemptibleVmsOnly || (tesResources?.Preemptible).GetValueOrDefault(true);

            List<VirtualMachineInformationWithDataDisks> eligibleVms = [];
            var noVmFoundMessage = string.Empty;

            var vmSize = tesResources?.GetBackendParameterValue(TesResources.SupportedBackendParameters.vm_size);

            if (!string.IsNullOrWhiteSpace(vmSize))
            {
                eligibleVms = virtualMachineInfoList
                    .Where(vm =>
                        vm.LowPriority == preemptible
                        && vm.VmSize.Equals(vmSize, StringComparison.OrdinalIgnoreCase))
                    .Select<VirtualMachineInformation, VirtualMachineInformationWithDataDisks>(vm => new(vm, []))
                    .ToList();

                noVmFoundMessage = $"No VM (out of {virtualMachineInfoList.Count}) available with the required resources (vmsize: {vmSize}, preemptible: {preemptible}) for task id {tesTask.Id}.";
            }
            else
            {
                var requiredNumberOfCores = (tesResources?.CpuCores).GetValueOrDefault(DefaultCoreCount);
                var requiredMemoryInGB = (tesResources?.RamGb).GetValueOrDefault(DefaultMemoryGb);
                var requiredDiskSizeInGB = (tesResources?.DiskGb).GetValueOrDefault(DefaultDiskGb);

                eligibleVms = await virtualMachineInfoList
                    .ToAsyncEnumerable()
                    .SelectAwaitWithCancellation<VirtualMachineInformation, VirtualMachineInformationWithDataDisks>(async (vm, token) => new(vm,
                        double.Max(vm.NvmeDiskSizeInGiB ?? 0, vm.ResourceDiskSizeInGiB ?? 0) < requiredDiskSizeInGB
                        ? await skuInformationProvider.GetStorageDisksAndPricesAsync(azureProxy.GetArmRegion(), requiredDiskSizeInGB, vm.MaxDataDiskCount ?? 0, token)
                        : []))
                    .Where(vm =>
                        vm.VM.LowPriority == preemptible
                        && vm.VM.VCpusAvailable >= requiredNumberOfCores
                        && vm.VM.MemoryInGiB >= requiredMemoryInGB
                        && (vm.DataDisks.Count != 0 || double.Max(vm.VM.NvmeDiskSizeInGiB ?? 0, vm.VM.ResourceDiskSizeInGiB ?? 0) >= requiredDiskSizeInGB)) // recheck because skuInformationProvider.GetStorageDisksAndPricesAsync can return an empty list
                    .ToListAsync(cancellationToken);

                noVmFoundMessage = $"No VM (out of {virtualMachineInfoList.Count}) available with the required resources (cores: {requiredNumberOfCores}, memory: {requiredMemoryInGB} GB, disk: {requiredDiskSizeInGB} GB, preemptible: {preemptible}) for task id {tesTask.Id}.";
            }


            var coreQuota = await quotaVerifier
                .GetBatchQuotaProvider()
                .GetVmCoreQuotaAsync(preemptible, cancellationToken);

            static decimal Cost(VirtualMachineInformationWithDataDisks vm) => (vm.VM.PricePerHour ?? 0M) + vm.DataDisks.Sum(disk => disk.PricePerHour);

            var selectedVm = eligibleVms
                .Where(AllowedVmSizesFilter)
                .Where(vm => IsThereSufficientCoreQuota(coreQuota, vm.VM))
                .Where(vm =>
                    !(previouslyFailedVmSizes?.Contains(vm.VM.VmSize, StringComparer.OrdinalIgnoreCase) ?? false))
                .MinBy(Cost);

            if (!preemptible && selectedVm is not null)
            {
                var idealVm = eligibleVms
                    .Where(AllowedVmSizesFilter)
                    .Where(vm => !(previouslyFailedVmSizes?.Contains(vm.VM.VmSize, StringComparer.OrdinalIgnoreCase) ?? false))
                    .MinBy(Cost);

                if (selectedVm.VM.PricePerHour >= idealVm.VM.PricePerHour * 2)
                {
                    tesTask.SetWarning("UsedLowPriorityInsteadOfDedicatedVm",
                        $"This task ran on low priority machine because dedicated quota was not available for VM Series '{idealVm.VM.VmFamily}'.",
                        $"Increase the quota for VM Series '{idealVm.VM.VmFamily}' to run this task on a dedicated VM. Please submit an Azure Support request to increase your quota: {AzureSupportUrl}");

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
                noVmFoundMessage += $" The following VM sizes were excluded from consideration because of {AzureBatchTaskState.TaskState.NodeAllocationFailed} error(s) on previous attempts: {string.Join(", ", previouslyFailedVmSizes)}.";
            }

            var vmsExcludedByTheAllowedVmsConfiguration = eligibleVms.Except(eligibleVms.Where(AllowedVmSizesFilter)).Count();

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
                    : null;
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

            Tes.Models.BatchNodeMetrics batchNodeMetrics = null;
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

                var metricsUrl = await storageAccessProvider.GetInternalTesTaskBlobUrlAsync(tesTask, "metrics.txt", BlobSasPermissions.Read, cancellationToken);
                var metricsContent = await storageAccessProvider.DownloadBlobAsync(metricsUrl, cancellationToken);

                if (metricsContent is not null)
                {
                    try
                    {
                        var metrics = DelimitedTextToDictionary(metricsContent.Trim());

                        var diskSizeInGB = TryGetValueAsDouble(metrics, "DiskSizeInKiB", out var diskSizeInKiB) ? diskSizeInKiB / kiBInGB : (double?)null;
                        var diskUsedInGB = TryGetValueAsDouble(metrics, "DiskUsedInKiB", out var diskUsedInKiB) ? diskUsedInKiB / kiBInGB : (double?)null;

                        batchNodeMetrics = new()
                        {
                            BlobXferImagePullDurationInSeconds = GetDurationInSeconds(metrics, "BlobXferPullStart", "BlobXferPullEnd"),
                            ExecutorImagePullDurationInSeconds = GetDurationInSeconds(metrics, "ExecutorPullStart", "ExecutorPullEnd"),
                            ExecutorImageSizeInGB = TryGetValueAsDouble(metrics, "ExecutorImageSizeInBytes", out var executorImageSizeInBytes) ? executorImageSizeInBytes / bytesInGB : null,
                            FileDownloadDurationInSeconds = GetDurationInSeconds(metrics, "DownloadStart", "DownloadEnd"),
                            FileDownloadSizeInGB = TryGetValueAsDouble(metrics, "FileDownloadSizeInBytes", out var fileDownloadSizeInBytes) ? fileDownloadSizeInBytes / bytesInGB : null,
                            ExecutorDurationInSeconds = GetDurationInSeconds(metrics, "ExecutorStart", "ExecutorEnd"),
                            FileUploadDurationInSeconds = GetDurationInSeconds(metrics, "UploadStart", "UploadEnd"),
                            FileUploadSizeInGB = TryGetValueAsDouble(metrics, "FileUploadSizeInBytes", out var fileUploadSizeInBytes) ? fileUploadSizeInBytes / bytesInGB : null,
                            DiskUsedInGB = diskUsedInGB,
                            DiskUsedPercent = diskUsedInGB.HasValue && diskSizeInGB.HasValue && diskSizeInGB > 0 ? (float?)(diskUsedInGB / diskSizeInGB * 100) : null,
                            VmCpuModelName = metrics.GetValueOrDefault("VmCpuModelName")
                        };

                        taskStartTime = TryGetValueAsDateTimeOffset(metrics, "BlobXferPullStart", out var startTime) ? startTime : null;
                        taskEndTime = TryGetValueAsDateTimeOffset(metrics, "UploadEnd", out var endTime) ? endTime : null;
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
            => text.Split(rowDelimiter, StringSplitOptions.RemoveEmptyEntries)
                .Select(line => { var parts = line.Split(fieldDelimiter, 2); return new KeyValuePair<string, string>(parts[0].Trim(), parts.Length < 2 ? string.Empty : parts[1]); })
                .ToDictionary(kv => kv.Key, kv => kv.Value);


        /// <inheritdoc/>
        public async IAsyncEnumerable<RunnerEventsMessage> GetEventMessagesAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken, string @event)
        {
            const string eventsFolderName = "events/";
            var prefix = eventsFolderName;

            if (!string.IsNullOrWhiteSpace(@event))
            {
                prefix += @event + "/";
            }

            var tesInternalSegments = StorageAccountUrlSegments.Create(storageAccessProvider.GetInternalTesBlobUrlWithoutSasToken(string.Empty).AbsoluteUri);
            var eventsStartIndex = (string.IsNullOrEmpty(tesInternalSegments.BlobName) ? string.Empty : (tesInternalSegments.BlobName + "/")).Length;
            var eventsEndIndex = eventsStartIndex + eventsFolderName.Length;

            await foreach (var blobItem in azureProxy.ListBlobsWithTagsAsync(
                    await storageAccessProvider.GetInternalTesBlobUrlAsync(
                        string.Empty,
                        BlobSasPermissions.Read | BlobSasPermissions.Tag | BlobSasPermissions.List,
                        cancellationToken),
                    prefix,
                    cancellationToken)
                .WithCancellation(cancellationToken))
            {
                if (blobItem.Tags.ContainsKey(RunnerEventsProcessor.ProcessedTag))
                {
                    continue;
                }

                var blobUrl = await storageAccessProvider.GetInternalTesBlobUrlAsync(blobItem.Name[eventsStartIndex..], BlobSasPermissions.Read | BlobSasPermissions.Write | BlobSasPermissions.Tag, cancellationToken);

                var pathFromEventName = blobItem.Name[eventsEndIndex..];
                var eventName = pathFromEventName[..pathFromEventName.IndexOf('/')];

                yield return new(blobUrl, blobItem.Tags, eventName);
            }
        }

        /// <summary>
        /// Class that captures how <see cref="TesTask"/> transitions from current state to the new state, given the current Batch task state and optional condition.
        /// Transitions typically include an action that needs to run in order for the task to move to the new state.
        /// </summary>
        private readonly struct TesTaskStateTransition
        {
            public TesTaskStateTransition(Predicate<TesTask> condition, AzureBatchTaskState.TaskState? batchTaskState, string alternateSystemLogItem, Func<TesTask, CombinedBatchTaskInfo, CancellationToken, Task<bool>> asyncAction)
                : this(condition, batchTaskState, alternateSystemLogItem, asyncAction, null)
            { }

            public TesTaskStateTransition(Predicate<TesTask> condition, AzureBatchTaskState.TaskState? batchTaskState, string alternateSystemLogItem, Func<TesTask, CombinedBatchTaskInfo, bool> action)
                : this(condition, batchTaskState, alternateSystemLogItem, null, action)
            {
            }

            private TesTaskStateTransition(Predicate<TesTask> condition, AzureBatchTaskState.TaskState? batchTaskState, string alternateSystemLogItem, Func<TesTask, CombinedBatchTaskInfo, CancellationToken, Task<bool>> asyncAction, Func<TesTask, CombinedBatchTaskInfo, bool> action)
            {
                Condition = condition;
                CurrentBatchTaskState = batchTaskState;
                AlternateSystemLogItem = alternateSystemLogItem;
                AsyncAction = asyncAction;
                Action = action;
            }

            public Predicate<TesTask> Condition { get; }
            public AzureBatchTaskState.TaskState? CurrentBatchTaskState { get; }
            private string AlternateSystemLogItem { get; }
            private Func<TesTask, CombinedBatchTaskInfo, CancellationToken, Task<bool>> AsyncAction { get; }
            private Func<TesTask, CombinedBatchTaskInfo, bool> Action { get; }

            /// <summary>
            /// Calls <see cref="Action"/> and/or <see cref="AsyncAction"/>.
            /// </summary>
            /// <param name="tesTask"></param>
            /// <param name="batchState"></param>
            /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
            /// <returns>True an action was called, otherwise False.</returns>
            public async ValueTask<bool> ActionAsync(TesTask tesTask, AzureBatchTaskState batchState, CancellationToken cancellationToken)
            {
                CombinedBatchTaskInfo combinedBatchTaskInfo = new(batchState, AlternateSystemLogItem);
                var tesTaskChanged = false;

                if (AsyncAction is not null)
                {
                    tesTaskChanged |= await AsyncAction(tesTask, combinedBatchTaskInfo, cancellationToken);
                }

                if (Action is not null)
                {
                    tesTaskChanged |= Action(tesTask, combinedBatchTaskInfo);
                }

                return tesTaskChanged;
            }
        }

        private record CombinedBatchTaskInfo : AzureBatchTaskState
        {
            /// <summary>
            /// Copy constructor that defaults <see cref="AzureBatchTaskState.State"/> (to enable hiding when serialized)
            /// </summary>
            /// <param name="original"><see cref="CombinedBatchTaskInfo"/> to copy</param>
            /// <param name="_1">Parameter that exists to not override the default copy constructor</param>
            public CombinedBatchTaskInfo(CombinedBatchTaskInfo original, bool _1)
                : this(original)
            {
                State = default;
            }

            /// <summary>
            /// SystemLog-appending copy constructor
            /// </summary>
            /// <param name="original"><see cref="CombinedBatchTaskInfo"/> to copy</param>
            /// <param name="additionalSystemLogItem">Text to add to the SystemLog in the copy</param>
            public CombinedBatchTaskInfo(CombinedBatchTaskInfo original, string additionalSystemLogItem)
                : base(original, additionalSystemLogItem)
            {
                AlternateSystemLogItem = original.AlternateSystemLogItem; // reattach this property
            }

            /// <summary>
            /// Constructor
            /// </summary>
            /// <param name="state"><see cref="AzureBatchTaskState"/> to extend</param>
            /// <param name="alternateSystemLogItem"><see cref="TesTaskStateTransition.AlternateSystemLogItem"/> from the selected Action</param>
            public CombinedBatchTaskInfo(AzureBatchTaskState state, string alternateSystemLogItem)
                : base(state)
            {
                AlternateSystemLogItem = alternateSystemLogItem;
            }

            public string AlternateSystemLogItem { get; set; }
        }

        internal record class VirtualMachineInformationWithDataDisks(VirtualMachineInformation VM, IList<Tes.Models.VmDataDisks> DataDisks)
        {
            /// <summary>
            /// Converts <see cref="Tes.Models.VmDataDisks"/> to <see cref="BatchModels.BatchVmDataDisk"/>.
            /// </summary>
            /// <param name="disk">The disk.</param>
            /// <returns></returns>
            public static BatchModels.BatchVmDataDisk ToBatchVmDataDisk(Tes.Models.VmDataDisks disk) => new(disk.Lun, disk.CapacityInGiB) { Caching = disk.Caching is null ? null : Enum.Parse<BatchModels.BatchDiskCachingType>(disk.Caching), StorageAccountType = Enum.Parse<BatchModels.BatchStorageAccountType>(disk.StorageAccountType) };
        }
    }
}
