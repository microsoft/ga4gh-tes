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
using Azure.Storage.Sas;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Primitives;
using Tes.Extensions;
using TesApi.Web.Events;
using TesApi.Web.Extensions;
using TesApi.Web.Management;
using TesApi.Web.Management.Models.Quotas;
using TesApi.Web.Runner;
using TesApi.Web.Storage;
using BatchModels = Microsoft.Azure.Management.Batch.Models;
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

        //[GeneratedRegex("[^\\?.]*(\\?.*)")]
        //private static partial Regex GetQueryStringRegex();

        /// <summary>
        /// Name of environment variable to place resources shared by all tasks on each compute node in a pool.
        /// </summary>
        public const string BatchNodeSharedEnvVar = "$AZ_BATCH_NODE_SHARED_DIR";

        private const string AzureSupportUrl = "https://portal.azure.com/#blade/Microsoft_Azure_Support/HelpAndSupportBlade/newsupportrequest";
        private const int PoolKeyLength = 55; // 64 max pool name length - 9 chars generating unique pool names
        private const int DefaultCoreCount = 1;
        private const int DefaultMemoryGb = 2;
        private const int DefaultDiskGb = 10;
        private const string CromwellScriptFileName = "script";
        private const string StartTaskScriptFilename = "start-task.sh";
        private const string NodeTaskRunnerFilename = "tes-runner";
        private const string NodeTaskRunnerMD5HashFilename = NodeTaskRunnerFilename + ".md5";
        private readonly string cromwellDrsLocalizerImageName;
        private readonly ILogger logger;
        private readonly IAzureProxy azureProxy;
        private readonly IStorageAccessProvider storageAccessProvider;
        private readonly IBatchQuotaVerifier quotaVerifier;
        private readonly IBatchSkuInformationProvider skuInformationProvider;
        private readonly IReadOnlyList<TesTaskStateTransition> tesTaskStateTransitions;
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
        private readonly Func<IBatchPool> batchPoolFactory;
        private readonly IAllowedVmSizesService allowedVmSizesService;
        private readonly TaskExecutionScriptingManager taskExecutionScriptingManager;
        private readonly string runnerMD5;

        /// <summary>
        /// Constructor for <see cref="BatchScheduler"/>
        /// </summary>
        /// <param name="logger">Logger <see cref="ILogger"/>.</param>
        /// <param name="batchGen1Options">Configuration of <see cref="Options.BatchImageGeneration1Options"/>.</param>
        /// <param name="batchGen2Options">Configuration of <see cref="Options.BatchImageGeneration2Options"/>.</param>
        /// <param name="marthaOptions">Configuration of <see cref="Options.MarthaOptions"/>.</param>
        /// <param name="storageOptions">Configuration of <see cref="Options.StorageOptions"/>.</param>
        /// <param name="batchNodesOptions">Configuration of <see cref="Options.BatchNodesOptions"/>.</param>
        /// <param name="batchSchedulingOptions">Configuration of <see cref="Options.BatchSchedulingOptions"/>.</param>
        /// <param name="azureProxy">Azure proxy <see cref="IAzureProxy"/>.</param>
        /// <param name="storageAccessProvider">Storage access provider <see cref="IStorageAccessProvider"/>.</param>
        /// <param name="quotaVerifier">Quota verifier <see cref="IBatchQuotaVerifier"/>.</param>
        /// <param name="skuInformationProvider">Sku information provider <see cref="IBatchSkuInformationProvider"/>.</param>
        /// <param name="poolFactory"><see cref="IBatchPool"/> factory.</param>
        /// <param name="allowedVmSizesService">Service to get allowed vm sizes.</param>
        /// <param name="taskExecutionScriptingManager"><see cref="TaskExecutionScriptingManager"/>.</param>
        public BatchScheduler(
            ILogger<BatchScheduler> logger,
            IOptions<Options.BatchImageGeneration1Options> batchGen1Options,
            IOptions<Options.BatchImageGeneration2Options> batchGen2Options,
            IOptions<Options.MarthaOptions> marthaOptions,
            IOptions<Options.StorageOptions> storageOptions,
            IOptions<Options.BatchNodesOptions> batchNodesOptions,
            IOptions<Options.BatchSchedulingOptions> batchSchedulingOptions,
            IAzureProxy azureProxy,
            IStorageAccessProvider storageAccessProvider,
            IBatchQuotaVerifier quotaVerifier,
            IBatchSkuInformationProvider skuInformationProvider,
            Func<IBatchPool> poolFactory,
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
            this.cromwellDrsLocalizerImageName = marthaOptions.Value.CromwellDrsLocalizer;
            if (string.IsNullOrWhiteSpace(this.cromwellDrsLocalizerImageName)) { this.cromwellDrsLocalizerImageName = Options.MarthaOptions.DefaultCromwellDrsLocalizer; }
            this.disableBatchNodesPublicIpAddress = batchNodesOptions.Value.DisablePublicIpAddress;
            this.poolLifetime = TimeSpan.FromDays(batchSchedulingOptions.Value.PoolRotationForcedDays == 0 ? Options.BatchSchedulingOptions.DefaultPoolRotationForcedDays : batchSchedulingOptions.Value.PoolRotationForcedDays);
            this.defaultStorageAccountName = storageOptions.Value.DefaultAccountName;
            logger.LogDebug(@"Default storage account: {DefaultStorageAccountName}", defaultStorageAccountName);
            this.globalStartTaskPath = StandardizeStartTaskPath(batchNodesOptions.Value.GlobalStartTask, this.defaultStorageAccountName);
            this.globalManagedIdentity = batchNodesOptions.Value.GlobalManagedIdentity;
            this.allowedVmSizesService = allowedVmSizesService;
            this.taskExecutionScriptingManager = taskExecutionScriptingManager;
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

            logger.LogInformation("usePreemptibleVmsOnly: {UsePreemptibleVmsOnly}", usePreemptibleVmsOnly);

            static bool tesTaskIsInitializingOrRunning(TesTask tesTask) => tesTask.State == TesState.INITIALIZINGEnum || tesTask.State == TesState.RUNNINGEnum;
            static bool tesTaskIsInitializing(TesTask tesTask) => tesTask.State == TesState.INITIALIZINGEnum;

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

                var (batchNodeMetrics, taskStartTime, taskEndTime, cromwellRcCode) = newTaskState == TesState.COMPLETEEnum
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

                    if (batchInfo.ExecutorEndTime is not null || batchInfo.ExecutorStartTime is not null || batchInfo.ExecutorExitCode is not null)
                    {
                        var tesTaskExecutorLog = tesTaskLog.GetOrAddExecutorLog();
                        tesTaskExecutorLog.StartTime ??= batchInfo.ExecutorStartTime;
                        tesTaskExecutorLog.EndTime ??= batchInfo.ExecutorEndTime;
                        tesTaskExecutorLog.ExitCode ??= batchInfo.ExecutorExitCode;
                    }

                    if (batchInfo.ReplaceBatchTaskStartTime)
                    {
                        tesTaskLog.StartTime = batchInfo.BatchTaskStartTime ?? taskStartTime;
                    }
                    else
                    {
                        tesTaskLog.StartTime ??= batchInfo.BatchTaskStartTime ?? taskStartTime;
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
                    else if (!(string.IsNullOrWhiteSpace(batchInfo.AlternateSystemLogItem) || tesTask.IsActiveState() || new[] { TesState.COMPLETEEnum, TesState.CANCELEDEnum }.Contains(tesTask.State)))
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
            }

            async Task<bool> SetCompletedWithErrors(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
            {
                var newTaskState = tesTask.FailureReason switch
                {
                    AzureBatchTaskState.ExecutorError => TesState.EXECUTORERROREnum,
                    _ => TesState.SYSTEMERROREnum,
                };

                return await SetTaskStateAndLogAsync(tesTask, newTaskState, batchInfo, cancellationToken);
            }

            async Task<bool> TerminateBatchTaskAndSetTaskStateAsync(TesTask tesTask, TesState newTaskState, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken) =>
                await TerminateBatchTaskAsync(tesTask, newTaskState, batchInfo, cancellationToken);

            Task<bool> TerminateBatchTaskAndSetTaskSystemErrorAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken) =>
                TerminateBatchTaskAndSetTaskStateAsync(tesTask, TesState.SYSTEMERROREnum, batchInfo, cancellationToken);

            Task<bool> RequeueTaskAfterFailureAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
                => ++tesTask.ErrorCount > 3
                    ? TerminateBatchTaskAndSetTaskSystemErrorAsync(tesTask, new(batchInfo, "System Error: Retry count exceeded."), cancellationToken)
                    : TerminateBatchTaskAndSetTaskStateAsync(tesTask, TesState.QUEUEDEnum, batchInfo, cancellationToken);

            async Task<bool> CancelTaskAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken) =>
                await TerminateBatchTaskAsync(tesTask, TesState.CANCELEDEnum, batchInfo, cancellationToken);

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
                tesTask.State = TesState.INITIALIZINGEnum;
                logger.LogInformation("The TesTask {TesTask}'s node was preempted. It was automatically rescheduled.", tesTask.Id);
                return true;
            }

            const string alternateSystemLogMissingFailure = "Please open an issue. There should have been an error reported here.";

            tesTaskStateTransitions =
            [
                new(condition: null, AzureBatchTaskState.TaskState.CancellationRequested, alternateSystemLogItem: null, CancelTaskAsync),
                new(tesTaskIsInitializing, AzureBatchTaskState.TaskState.NodeAllocationFailed, alternateSystemLogItem: null, RequeueTaskAfterFailureAsync),
                new(tesTaskIsInitializing, AzureBatchTaskState.TaskState.NodeStartTaskFailed, alternateSystemLogMissingFailure, TerminateBatchTaskAndSetTaskSystemErrorAsync),
                new(tesTaskIsInitializingOrRunning, AzureBatchTaskState.TaskState.Initializing, alternateSystemLogItem: null, (tesTask, info, ct) => SetTaskStateAndLogAsync(tesTask, TesState.INITIALIZINGEnum, info, ct)),
                new(tesTaskIsInitializingOrRunning, AzureBatchTaskState.TaskState.Running, alternateSystemLogItem: null, (tesTask, info, ct) => SetTaskStateAndLogAsync(tesTask, TesState.RUNNINGEnum, info, ct)),
                new(tesTaskIsInitializingOrRunning, AzureBatchTaskState.TaskState.CompletedSuccessfully, alternateSystemLogItem: null, (tesTask, info, ct) => SetTaskStateAndLogAsync(tesTask, TesState.COMPLETEEnum, info, ct)),
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
                    await batchPoolFactory().AssignPoolAsync(cloudPool, forceRemove, cancellationToken);
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
            if (!runnerMD5.Equals(blobProperties is null ? string.Empty : Convert.ToBase64String(blobProperties.ContentHash), StringComparison.OrdinalIgnoreCase))
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

        private static string GetCromwellExecutionDirectoryPathAsUrl(TesTask task)
        {
            var commandScript = task.Inputs?.FirstOrDefault(IsCromwellCommandScript);
            return commandScript switch
            {
                null => null,
                var x when string.IsNullOrEmpty(x.Content) => GetParentUrl(commandScript.Url),
                _ => GetParentPath(commandScript.Path).TrimStart('/')
            };
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

        /// <summary>
        /// Determines if the <see cref="Tes.Models.TesInput"/> file is a Cromwell command script
        /// See https://github.com/broadinstitute/cromwell/blob/17efd599d541a096dc5704991daeaefdd794fefd/supportedBackends/tes/src/main/scala/cromwell/backend/impl/tes/TesTask.scala#L58
        /// </summary>
        /// <param name="inputFile"><see cref="Tes.Models.TesInput"/> file</param>
        /// <returns>True if the file is a Cromwell command script</returns>
        private static bool IsCromwellCommandScript(TesInput inputFile)
            => (inputFile.Name?.Equals("commandScript") ?? false) && (inputFile.Description?.EndsWith(".commandScript") ?? false) && inputFile.Type == TesFileType.FILEEnum && inputFile.Path.EndsWith($"/{CromwellScriptFileName}");

        private record struct QueuedTaskPoolMetadata(TesTask TesTask, VirtualMachineInformation VirtualMachineInfo, IEnumerable<string> Identities, string PoolDisplayName);
        private record struct QueuedTaskJobMetadata(string PoolKey, string JobId, VirtualMachineInformation VirtualMachineInfo, IEnumerable<TesTask> Tasks);
        private record struct QueuedTaskMetadata(string PoolKey, IEnumerable<TesTask> Tasks);

        /// <inheritdoc/>
        public async IAsyncEnumerable<RelatedTask<TesTask, bool>> ProcessQueuedTesTasksAsync(TesTask[] tesTasks, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            ConcurrentBag<RelatedTask<TesTask, bool>> results = []; // Early item return facilitator. TaskCatchException() & TaskCatchAggregateException() add items to this.
            ConcurrentDictionary<string, ImmutableArray<QueuedTaskPoolMetadata>> tasksPoolMetadataByPoolKey = new();

            {
                logger.LogDebug(@"Checking quota for {QueuedTasks} tasks.", tesTasks.Length);

                // Determine how many nodes in each pool we might need for this group.
                await Parallel.ForEachAsync(tesTasks, cancellationToken, async (tesTask, token) =>
                {
                    string poolKey = default;
                    var identities = new List<string>();

                    if (!string.IsNullOrWhiteSpace(globalManagedIdentity))
                    {
                        identities.Add(globalManagedIdentity);
                    }

                    if (tesTask.Resources?.ContainsBackendParameterValue(TesResources.SupportedBackendParameters.workflow_execution_identity) == true)
                    {
                        identities.Add(tesTask.Resources?.GetBackendParameterValue(TesResources.SupportedBackendParameters.workflow_execution_identity));
                    }

                    try
                    {
                        var virtualMachineInfo = await GetVmSizeAsync(tesTask, token);
                        (poolKey, var displayName) = GetPoolKey(tesTask, virtualMachineInfo, identities);
                        await quotaVerifier.CheckBatchAccountQuotasAsync(virtualMachineInfo, needPoolOrJobQuotaCheck: !IsPoolAvailable(poolKey), cancellationToken: token);

                        _ = tasksPoolMetadataByPoolKey.AddOrUpdate(poolKey,
                            _ => [new(tesTask, virtualMachineInfo, identities, displayName)],
                            (_, list) => list.Add(new(tesTask, virtualMachineInfo, identities, displayName)));
                    }
                    catch (Exception exception)
                    {
                        TaskCatchException(exception, tesTask, poolKey);
                    }
                });
            }

            // Return any results that are ready
            foreach (var result in results)
            {
                yield return result;
            }

            if (tasksPoolMetadataByPoolKey.IsEmpty)
            {
                yield break;
            }

            results.Clear();

            // Determine how many nodes in each possibly new pool we might need for this group of tasks.
            var neededPoolNodesByPoolKey = tasksPoolMetadataByPoolKey.ToDictionary(t => t.Key, t => t.Value.Length);
            ConcurrentBag<QueuedTaskJobMetadata> tasksJobMetadata = [];

            {
                // Determine how many new pools/jobs we need now
                var requiredNewPools = neededPoolNodesByPoolKey.Keys.WhereNot(IsPoolAvailable).ToArray();

                // Revisit pool/job quotas (the above loop already dealt with the possiblility of needing just one more pool or job).
                // This will remove pool keys we cannot accomodate due to quota, along with all of their associated tasks, from being queued into Batch.
                if (requiredNewPools.Skip(1).Any())
                {
                    bool TryRemoveKeyAndTasks(string key, out (string Key, ImmutableArray<QueuedTaskPoolMetadata> ListOfTaskMetadata) result)
                    {
                        result = default;

                        if (tasksPoolMetadataByPoolKey.TryRemove(key, out var listOfTaskMetadata))
                        {
                            result = (key, listOfTaskMetadata);
                            return true;
                        }

                        return false;
                    }

                    var (exceededQuantity, exception) = await quotaVerifier.CheckBatchAccountPoolAndJobQuotasAsync(requiredNewPools.Length, cancellationToken);

                    foreach (var (key, listOfTaskMetadata) in requiredNewPools
                        .Reverse() // TODO: do we want to favor earlier or later tasks?
                        .SelectWhere<string, (string, ImmutableArray<QueuedTaskPoolMetadata>)>(TryRemoveKeyAndTasks)
                        .Take(exceededQuantity))
                    {
                        foreach (var task in listOfTaskMetadata.Select(m => m.TesTask))
                        {
                            yield return new(HandleExceptionAsync(exception, key, task), task);
                        }
                    }
                }

                logger.LogDebug(@"Obtaining {PoolQuantity} batch pool identifiers for {QueuedTasks} tasks.", tasksPoolMetadataByPoolKey.Count, tasksPoolMetadataByPoolKey.Values.Sum(l => l.Length));

                await Parallel.ForEachAsync(tasksPoolMetadataByPoolKey, cancellationToken, async (pool, token) =>
                {
                    var (_, virtualMachineInfo, identities, displayName) = pool.Value.First();

                    try
                    {
                        var useGen2 = virtualMachineInfo.HyperVGenerations?.Contains("V2", StringComparer.OrdinalIgnoreCase) ?? false;
                        var poolId = (await GetOrAddPoolAsync(
                            key: pool.Key,
                            isPreemptable: virtualMachineInfo.LowPriority,
                            modelPoolFactory: async (id, ct) => await GetPoolSpecification(
                                name: id,
                                displayName: displayName,
                                poolIdentity: GetBatchPoolIdentity(identities),
                                vmSize: virtualMachineInfo.VmSize,
                                preemptable: virtualMachineInfo.LowPriority,
                                initialTarget: neededPoolNodesByPoolKey[pool.Key],
                                nodeInfo: useGen2 ? gen2BatchNodeInfo : gen1BatchNodeInfo,
                                encryptionAtHostSupported: virtualMachineInfo.EncryptionAtHostSupported,
                                cancellationToken: ct),
                            cancellationToken: token)).PoolId;

                        tasksJobMetadata.Add(new(pool.Key, poolId, virtualMachineInfo, pool.Value.Select(tuple => tuple.TesTask)));
                    }
                    catch (AggregateException aggregateException)
                    {
                        var innerExceptions = aggregateException.Flatten().InnerExceptions;

                        foreach (var tesTask in pool.Value.Select(tuple => tuple.TesTask))
                        {
                            TaskCatchAggregateException(innerExceptions, tesTask, pool.Key);
                        }
                    }
                    catch (Exception exception)
                    {
                        foreach (var tesTask in pool.Value.Select(tuple => tuple.TesTask))
                        {
                            TaskCatchException(exception, tesTask, pool.Key);
                        }
                    }
                });
            }

            // Return any results that are ready
            foreach (var result in results)
            {
                yield return result;
            }

            if (tasksJobMetadata.IsEmpty)
            {
                yield break;
            }

            results.Clear();

            ConcurrentBag<QueuedTaskMetadata> tasksMetadata = [];

            async Task<CloudTask> GetCloudTaskAsync(TesTask tesTask, VirtualMachineInformation virtualMachineInfo, string poolKey, string poolId, CancellationToken cancellationToken)
            {
                try
                {
                    var tesTaskLog = tesTask.AddTesTaskLog();
                    tesTaskLog.VirtualMachineInfo = virtualMachineInfo;
                    var cloudTaskId = $"{tesTask.Id}-{tesTask.Logs.Count}";
                    tesTask.PoolId = poolId;
                    var cloudTask = await ConvertTesTaskToBatchTaskUsingRunnerAsync(cloudTaskId, tesTask, cancellationToken);

                    logger.LogInformation(@"Creating batch task for TES task {TesTaskId}. Using VM size {VmSize}.", tesTask.Id, virtualMachineInfo.VmSize);
                    return cloudTask;
                }
                catch (AggregateException aggregateException)
                {
                    TaskCatchAggregateException(aggregateException.Flatten().InnerExceptions, tesTask, poolKey);
                }
                catch (Exception exception)
                {
                    TaskCatchException(exception, tesTask, poolKey);
                }

                return null;
            }

            await Parallel.ForEachAsync(
                tasksJobMetadata.Select(metadata => (metadata.JobId, metadata.PoolKey, metadata.Tasks, CloudTasks: metadata.Tasks
                    .Select(task => new RelatedTask<TesTask, CloudTask>(GetCloudTaskAsync(task, metadata.VirtualMachineInfo, metadata.PoolKey, metadata.JobId, cancellationToken), task))
                    .WhenEach(cancellationToken, task => task.Task))),
                cancellationToken,
                async (metadata, token) =>
                {
                    var (jobId, poolKey, tasks, relatedCloudTasks) = metadata;

                    try
                    {
                        var cloudTasks = (await relatedCloudTasks.ToListAsync(token)).Where(task => task.Task.Result is not null);
                        await azureProxy.AddBatchTasksAsync(cloudTasks.Select(task => task.Task.Result), jobId, token);

                        tasksMetadata.Add(new(poolKey, cloudTasks.Select(task => task.Related)));
                    }
                    catch (AggregateException aggregateException)
                    {
                        var innerExceptions = aggregateException.Flatten().InnerExceptions;

                        foreach (var tesTask in tasks)
                        {
                            TaskCatchAggregateException(innerExceptions, tesTask, poolKey);
                        }
                    }
                    catch (Exception exception)
                    {
                        foreach (var tesTask in tasks)
                        {
                            TaskCatchException(exception, tesTask, poolKey);
                        }
                    }
                });

            // Return any results that are ready
            foreach (var result in results)
            {
                yield return result;
            }

            if (tasksMetadata.IsEmpty)
            {
                yield break;
            }

            results.Clear();

            _ = Parallel.ForEach(tasksMetadata.SelectMany(metadata => metadata.Tasks.Select(task => (task, metadata.PoolKey))), metadata =>
            {
                var (tesTask, poolKey) = metadata;

                try
                {
                    var tesTaskLog = tesTask.GetOrAddTesTaskLog();
                    tesTaskLog.StartTime = DateTimeOffset.UtcNow;
                    tesTask.State = TesState.INITIALIZINGEnum;
                    results.Add(new(Task.FromResult(true), tesTask));
                }
                catch (AggregateException aggregateException)
                {
                    TaskCatchAggregateException(aggregateException.Flatten().InnerExceptions, tesTask, poolKey);
                }
                catch (Exception exception)
                {
                    TaskCatchException(exception, tesTask, poolKey);
                }
            });

            foreach (var result in results)
            {
                yield return result;
            }

            yield break;

            void TaskCatchException(Exception exception, TesTask tesTask, string poolKey)
            {
                results.Add(new(HandleExceptionAsync(exception, poolKey, tesTask), tesTask));
            }

            void TaskCatchAggregateException(IEnumerable<Exception> innerExceptions, TesTask tesTask, string poolKey)
            {
                var result = false;
                var exceptions = new List<Exception>();

                foreach (var partResult in innerExceptions
                    .Select(ex => HandleExceptionAsync(ex, poolKey, tesTask)))
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

                results.Add(new(exceptions.Count == 0
                    ? Task.FromResult(result)
                    : Task.FromException<bool>(new AggregateException(exceptions)),
                    tesTask));
            }

            Task<bool> HandleExceptionAsync(Exception exception, string poolKey, TesTask tesTask)
            {
                switch (exception)
                {
                    case AzureBatchPoolCreationException azureBatchPoolCreationException:
                        if (!azureBatchPoolCreationException.IsTimeout && !azureBatchPoolCreationException.IsJobQuota && !azureBatchPoolCreationException.IsPoolQuota && azureBatchPoolCreationException.InnerException is not null)
                        {
                            return HandleExceptionAsync(azureBatchPoolCreationException.InnerException, poolKey, tesTask);
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
                            });
                        }

                        break;

                    case AzureBatchQuotaMaxedOutException azureBatchQuotaMaxedOutException:
                        logger.LogWarning("TES task: {TesTask} AzureBatchQuotaMaxedOutException.Message: {ExceptionMessage}. Not enough quota available. Task will remain with state QUEUED.", tesTask.Id, azureBatchQuotaMaxedOutException.Message);
                        neededPools.Add(poolKey);
                        break;

                    case AzureBatchLowQuotaException azureBatchLowQuotaException:
                        tesTask.State = TesState.SYSTEMERROREnum;
                        tesTask.AddTesTaskLog(); // Adding new log here because this exception is thrown from CheckBatchAccountQuotas() and AddTesTaskLog() above is called after that. This way each attempt will have its own log entry.
                        tesTask.SetFailureReason("InsufficientBatchQuota", azureBatchLowQuotaException.Message);
                        logger.LogError(azureBatchLowQuotaException, "TES task: {TesTask} AzureBatchLowQuotaException.Message: {ExceptionMessage}", tesTask.Id, azureBatchLowQuotaException.Message);
                        break;

                    case AzureBatchVirtualMachineAvailabilityException azureBatchVirtualMachineAvailabilityException:
                        tesTask.State = TesState.SYSTEMERROREnum;
                        tesTask.AddTesTaskLog(); // Adding new log here because this exception is thrown from GetVmSizeAsync() and AddTesTaskLog() above is called after that. This way each attempt will have its own log entry.
                        tesTask.SetFailureReason("NoVmSizeAvailable", azureBatchVirtualMachineAvailabilityException.Message);
                        logger.LogError(azureBatchVirtualMachineAvailabilityException, "TES task: {TesTask} AzureBatchVirtualMachineAvailabilityException.Message: {ExceptionMessage}", tesTask.Id, azureBatchVirtualMachineAvailabilityException.Message);
                        break;

                    case TesException tesException:
                        tesTask.State = TesState.SYSTEMERROREnum;
                        tesTask.SetFailureReason(tesException);
                        logger.LogError(tesException, "TES task: {TesTask} TesException.Message: {ExceptionMessage}", tesTask.Id, tesException.Message);
                        break;

                    case BatchClientException batchClientException:
                        tesTask.State = TesState.SYSTEMERROREnum;
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
                        tesTask.State = TesState.SYSTEMERROREnum;
                        tesTask.SetFailureReason(AzureBatchTaskState.UnknownError, $"{exception?.GetType().FullName}: {exception?.Message}", exception?.StackTrace);
                        logger.LogError(exception, "TES task: {TesTask} Exception: {ExceptionType}: {ExceptionMessage}", tesTask.Id, exception?.GetType().FullName, exception?.Message);
                        break;
                }

                return Task.FromResult(true);
            }
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
                GlobalManagedIdentity: globalManagedIdentity
            );
            return nodeTaskCreationOptions;
        }

        private async ValueTask<IList<TesInput>> GetAdditionalCromwellInputsAsync(TesTask task, CancellationToken cancellationToken)
        {
            var cromwellExecutionDirectoryUrl = GetCromwellExecutionDirectoryPathAsUrl(task);

            // TODO: Cromwell bug: Cromwell command write_tsv() generates a file in the execution directory, for example execution/write_tsv_3922310b441805fc43d52f293623efbc.tmp. These are not passed on to TES inputs.
            // WORKAROUND: Get the list of files in the execution directory and add them to task inputs.

            return (string.IsNullOrWhiteSpace(cromwellExecutionDirectoryUrl)
                    ? default
                    : await GetExistingBlobsInCromwellStorageLocationAsTesInputsAsync(task, cromwellExecutionDirectoryUrl, cancellationToken))
                ?? [];
        }

        private async ValueTask<List<TesInput>> GetExistingBlobsInCromwellStorageLocationAsTesInputsAsync(TesTask task,
            string cromwellExecutionDirectoryUrl, CancellationToken cancellationToken)
        {
            var scriptInput = task.Inputs!.FirstOrDefault(IsCromwellCommandScript);
            var scriptPath = scriptInput!.Path;

            if (!Uri.TryCreate(cromwellExecutionDirectoryUrl, UriKind.Absolute, out _))
            {
                cromwellExecutionDirectoryUrl = $"/{cromwellExecutionDirectoryUrl}";
            }

            var executionDirectoryUri = await storageAccessProvider.MapLocalPathToSasUrlAsync(cromwellExecutionDirectoryUrl, BlobSasPermissions.List, cancellationToken);

            if (executionDirectoryUri is not null)
            {
                var executionDirectoryBlobName = new Azure.Storage.Blobs.BlobUriBuilder(executionDirectoryUri).BlobName;
                var pathBlobPrefix = scriptPath[..scriptPath.IndexOf(executionDirectoryBlobName, StringComparison.OrdinalIgnoreCase)];

                var blobsInExecutionDirectory =
                    await azureProxy.ListBlobsAsync(executionDirectoryUri, cancellationToken)
                        .Select(info => (Path: $"{pathBlobPrefix}{info.BlobName}", Uri: info.BlobUri))
                        .ToListAsync(cancellationToken);

                var scriptBlob =
                    blobsInExecutionDirectory.FirstOrDefault(b => scriptPath.Equals(b.Path, StringComparison.Ordinal));

                var expectedPathParts = scriptPath.Split('/').Length;

                return blobsInExecutionDirectory
                    .Where(b => b != scriptBlob)
                    .Where(b => b.Path.Split('/').Length == expectedPathParts)
                    .Select(b => new TesInput
                    {
                        Path = b.Path,
                        Url = b.Uri.AbsoluteUri,
                        Name = Path.GetFileName(b.Path),
                        Type = TesFileType.FILEEnum
                    })
                    .ToList();
            }

            return default;
        }

        private static void ValidateTesTask(TesTask task)
        {
            ArgumentNullException.ThrowIfNull(task);

            task.Inputs?.ForEach(input => ValidateTesTaskInput(input, task));
        }

        private static void ValidateTesTaskInput(TesInput inputFile, TesTask tesTask)
        {
            if (string.IsNullOrWhiteSpace(inputFile.Path) || !inputFile.Path.StartsWith('/'))
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

            if (inputFile.Type == TesFileType.DIRECTORYEnum)
            {
                throw new TesException("InvalidInputFilePath", "Directory input is not supported.");
            }
        }

        /// <summary>
        /// Constructs a universal Azure Start Task instance
        /// </summary>
        /// <param name="poolId">Pool Id</param>
        /// <param name="machineConfiguration">A <see cref="BatchModels.VirtualMachineConfiguration"/> describing the OS of the pool's nodes.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        /// <remarks>This method also mitigates errors associated with docker daemons that are not configured to place their filesystem assets on the data drive.</remarks>
        private async Task<BatchModels.StartTask> GetStartTaskAsync(string poolId, BatchModels.VirtualMachineConfiguration machineConfiguration, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(poolId);
            ArgumentNullException.ThrowIfNull(machineConfiguration);

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
            cmd.Append($"mkdir -p {BatchNodeSharedEnvVar} && {CreateWgetDownloadCommand(await storageAccessProvider.GetInternalTesBlobUrlAsync(NodeTaskRunnerFilename, BlobSasPermissions.Read, cancellationToken), $"{BatchNodeSharedEnvVar}/{NodeTaskRunnerFilename}", setExecutable: true)}");

            if (!dockerConfigured)
            {
                var commandLine = new StringBuilder("#!/usr/bin/bash\n");
                commandLine.Append(@"trap ""echo Error trapped; exit 0"" ERR; sudo touch tmp2.json && (sudo cp /etc/docker/daemon.json tmp1.json || sudo echo {} > tmp1.json) && sudo chmod a+w tmp?.json && if fgrep ""$(dirname ""$(dirname ""$AZ_BATCH_NODE_ROOT_DIR"")"")/docker"" tmp1.json; then echo grep ""found docker path""; elif [ $? -eq 1 ]; then ");

                commandLine.Append(machineConfiguration.NodeAgentSkuId switch
                {
                    var s when s.StartsWith("batch.node.ubuntu ") => "sudo apt-get install -y jq",
                    var s when s.StartsWith("batch.node.centos ") => "sudo yum install epel-release -y && sudo yum update -y && sudo yum install -y jq wget",
                    _ => throw new InvalidOperationException($"Unrecognized OS. Please send open an issue @ 'https://github.com/microsoft/ga4gh-tes/issues' with this message: Please add support for '{machineConfiguration.NodeAgentSkuId}'")
                });

                commandLine.Append(@" && jq \.\[\""data-root\""\]=\""""$(dirname ""$(dirname ""$AZ_BATCH_NODE_ROOT_DIR"")"")/docker""\"" tmp1.json >> tmp2.json && sudo cp tmp2.json /etc/docker/daemon.json && sudo chmod 644 /etc/docker/daemon.json && sudo systemctl restart docker && echo ""updated docker data-root""; else (echo ""grep failed"" || exit 1); fi'");

                var script = "config-docker.sh";
                cmd.Append($" && {CreateWgetDownloadCommand(await UploadScriptAsync(script, commandLine), script, setExecutable: true)} && ./{script}");
            }

            if (globalStartTaskConfigured)
            {
                cmd.Append($" && {CreateWgetDownloadCommand(globalStartTaskSasUrl, $"global-{StartTaskScriptFilename}", setExecutable: true)} && ./global-{StartTaskScriptFilename}");
            }

            return new()
            {
                CommandLine = $"/bin/sh -c \"{CreateWgetDownloadCommand(await UploadScriptAsync(StartTaskScriptFilename, cmd), StartTaskScriptFilename, true)} && ./{StartTaskScriptFilename}\"",
                UserIdentity = new BatchModels.UserIdentity(autoUser: new(elevationLevel: BatchModels.ElevationLevel.Admin, scope: BatchModels.AutoUserScope.Pool)),
                MaxTaskRetryCount = 4,
                WaitForSuccess = true
            };

            async ValueTask<Uri> UploadScriptAsync(string name, StringBuilder content)
            {
                content.AppendLinuxLine(string.Empty);
                var path = $"/pools/{poolId}/{name}";
                await azureProxy.UploadBlobAsync(await storageAccessProvider.GetInternalTesBlobUrlAsync(path, BlobSasPermissions.Write, cancellationToken), content.ToString(), cancellationToken);
                content.Clear();
                return await storageAccessProvider.GetInternalTesBlobUrlAsync(path, BlobSasPermissions.Read, cancellationToken);
            }
        }

        /// <summary>
        /// Generate the BatchPoolIdentity object
        /// </summary>
        /// <param name="identities"></param>
        /// <returns></returns>
        private static BatchModels.BatchPoolIdentity GetBatchPoolIdentity(IEnumerable<string> identities)
        {
            identities = identities?.ToList();
            return identities is null || !identities.Any()
                ? null
                : new(BatchModels.PoolIdentityType.UserAssigned,
                    identities.ToDictionary(identity => identity, _ => new BatchModels.UserAssignedIdentities()));
        }

        /// <summary>
        /// Generate the <see cref="BatchModels.Pool"/> for the needed pool.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="displayName"></param>
        /// <param name="poolIdentity"></param>
        /// <param name="vmSize"></param>
        /// <param name="preemptable"></param>
        /// <param name="initialTarget"></param>
        /// <param name="nodeInfo"></param>
        /// <param name="encryptionAtHostSupported">VM supports encryption at host.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>The specification for the pool.</returns>
        /// <remarks>
        /// Devs: Any changes to any properties set in this method will require corresponding changes to all classes implementing <see cref="Management.Batch.IBatchPoolManager"/> along with possibly any systems they call, with the possible exception of <seealso cref="Management.Batch.ArmBatchPoolManager"/>.
        /// </remarks>
        private async ValueTask<BatchModels.Pool> GetPoolSpecification(string name, string displayName, BatchModels.BatchPoolIdentity poolIdentity, string vmSize, bool preemptable, int initialTarget, BatchNodeInfo nodeInfo, bool? encryptionAtHostSupported, CancellationToken cancellationToken)
        {
            // TODO: (perpetually) add new properties we set in the future on <see cref="PoolSpecification"/> and/or its contained objects, if possible. When not, update CreateAutoPoolModePoolInformation().

            ValidateString(name, 64);
            ValidateString(displayName, 1024);

            BatchModels.VirtualMachineConfiguration vmConfig = new(
                imageReference: new(
                    publisher: nodeInfo.BatchImagePublisher,
                    offer: nodeInfo.BatchImageOffer,
                    sku: nodeInfo.BatchImageSku,
                    version: nodeInfo.BatchImageVersion),
                nodeAgentSkuId: nodeInfo.BatchNodeAgentSkuId);

            if (encryptionAtHostSupported ?? false)
            {
                vmConfig.DiskEncryptionConfiguration = new(
                    targets: [BatchModels.DiskEncryptionTarget.OsDisk, BatchModels.DiskEncryptionTarget.TemporaryDisk]
                );
            }

            BatchModels.Pool poolSpecification = new(name: name, displayName: displayName, identity: poolIdentity, vmSize: vmSize)
            {
                ScaleSettings = new(autoScale: new(BatchPool.AutoPoolFormula(preemptable, initialTarget), BatchPool.AutoScaleEvaluationInterval)),
                DeploymentConfiguration = new(virtualMachineConfiguration: vmConfig),
                //ApplicationPackages = ,
                StartTask = await GetStartTaskAsync(name, vmConfig, cancellationToken),
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
        public async Task<VirtualMachineInformation> GetVmSizeAsync(TesTask tesTask, CancellationToken cancellationToken, bool forcePreemptibleVmsOnly = false)
        {
            var allowedVmSizes = await allowedVmSizesService.GetAllowedVmSizes(cancellationToken);
            bool AllowedVmSizesFilter(VirtualMachineInformation vm) => allowedVmSizes is null || !allowedVmSizes.Any() || allowedVmSizes.Contains(vm.VmSize, StringComparer.OrdinalIgnoreCase) || allowedVmSizes.Contains(vm.VmFamily, StringComparer.OrdinalIgnoreCase);

            var tesResources = tesTask.Resources;

            var previouslyFailedVmSizes = tesTask.Logs?
                .Where(log => log.FailureReason == AzureBatchTaskState.TaskState.NodeAllocationFailed.ToString() && log.VirtualMachineInfo?.VmSize is not null)
                .Select(log => log.VirtualMachineInfo.VmSize)
                .Distinct()
                .ToList();

            var virtualMachineInfoList = await skuInformationProvider.GetVmSizesAndPricesAsync(azureProxy.GetArmRegion(), cancellationToken);
            var preemptible = forcePreemptibleVmsOnly || usePreemptibleVmsOnly || (tesResources?.Preemptible).GetValueOrDefault(true);

            List<VirtualMachineInformation> eligibleVms = [];
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
                .Where(AllowedVmSizesFilter)
                .Where(vm => IsThereSufficientCoreQuota(coreQuota, vm))
                .Where(vm =>
                    !(previouslyFailedVmSizes?.Contains(vm.VmSize, StringComparer.OrdinalIgnoreCase) ?? false))
                .MinBy(vm => vm.PricePerHour);

            if (!preemptible && selectedVm is not null)
            {
                var idealVm = eligibleVms
                    .Where(AllowedVmSizesFilter)
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
                var cromwellExecutionDirectoryPath = GetCromwellExecutionDirectoryPathAsUrl(tesTask);
                string cromwellRcContentPath;

                if (Uri.TryCreate(cromwellExecutionDirectoryPath, UriKind.Absolute, out _))
                {
                    var cromwellRcContentBuilder = new UriBuilder(cromwellExecutionDirectoryPath);
                    cromwellRcContentBuilder.Path += "/rc";
                    cromwellRcContentPath = cromwellRcContentBuilder.Uri.AbsoluteUri;
                }
                else
                {
                    cromwellRcContentPath = $"/{cromwellExecutionDirectoryPath}/rc";
                }

                var cromwellRcContent = await storageAccessProvider.DownloadBlobAsync(cromwellRcContentPath, cancellationToken);

                if (cromwellRcContent is not null && int.TryParse(cromwellRcContent, out var temp))
                {
                    cromwellRcCode = temp;
                }

                var metricsPath = await storageAccessProvider.GetInternalTesTaskBlobUrlAsync(tesTask, "metrics.txt", BlobSasPermissions.Read, cancellationToken);
                var metricsContent = await storageAccessProvider.DownloadBlobAsync(metricsPath.AbsoluteUri, cancellationToken);

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
                        logger.LogError(@"Failed to parse metrics for task {TesTask}. Error: {ExceptionMessage}", tesTask.Id, ex.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogError(@"Failed to get batch node metrics for task {TesTask}. Error: {ExceptionMessage}", tesTask.Id, ex.Message);
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
    }
}
