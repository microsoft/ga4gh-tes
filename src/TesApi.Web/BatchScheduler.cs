// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.


using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Tes.Extensions;
using TesApi.Web.Events;
using TesApi.Web.Extensions;
using TesApi.Web.Management;
using TesApi.Web.Management.Models.Quotas;
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
    /// Orchestrates <see cref="TesTask"/>s on Azure Batch
    /// </summary>
    public partial class BatchScheduler : IBatchScheduler
    {
        internal const string PoolHostName = "CoA-TES-HostName";
        internal const string PoolIsDedicated = "CoA-TES-IsDedicated";

        [GeneratedRegex("[^\\?.]*(\\?.*)")]
        private static partial Regex GetQueryStringRegex();

        private const string AzureSupportUrl = "https://portal.azure.com/#blade/Microsoft_Azure_Support/HelpAndSupportBlade/newsupportrequest";
        private const int PoolKeyLength = 55; // 64 max pool name length - 9 chars generating unique pool names
        private const int DefaultCoreCount = 1;
        private const int DefaultMemoryGb = 2;
        private const int DefaultDiskGb = 10;
        private const string TesExecutionsPathPrefix = "/tes-internal";
        private const string CromwellScriptFileName = "script";
        private const string StartTaskScriptFilename = "start-task.sh";
        private const string NodeTaskRunnerFilename = "tes-runner";
        private const string NodeTaskRunnerMD5HashFilename = "tes-runnerMD5hash.txt";
        private static readonly Regex queryStringRegex = GetQueryStringRegex();
        private readonly string dockerInDockerImageName;
        private readonly string cromwellDrsLocalizerImageName;
        private readonly ILogger logger;
        private readonly IAzureProxy azureProxy;
        private readonly IStorageAccessProvider storageAccessProvider;
        private readonly IBatchQuotaVerifier quotaVerifier;
        private readonly IBatchSkuInformationProvider skuInformationProvider;
        private readonly IList<TesTaskStateTransition> tesTaskStateTransitions;
        private readonly bool usePreemptibleVmsOnly;
        private readonly string batchNodesSubnetId;
        private readonly bool disableBatchNodesPublicIpAddress;
        private readonly TimeSpan poolLifetime;
        private readonly BatchNodeInfo gen2BatchNodeInfo;
        private readonly BatchNodeInfo gen1BatchNodeInfo;
        private readonly string defaultStorageAccountName;
        private readonly string globalStartTaskPath;
        private readonly string globalManagedIdentity;
        private readonly ContainerRegistryProvider containerRegistryProvider;
        private readonly string batchPrefix;
        private readonly Func<IBatchPool> batchPoolFactory;
        private readonly IAllowedVmSizesService allowedVmSizesService;
        private readonly TaskExecutionScriptingManager taskExecutionScriptingManager;

        /// <summary>
        /// Constructor for <see cref="BatchScheduler"/>
        /// </summary>
        /// <param name="logger">Logger <see cref="ILogger"/>.</param>
        /// <param name="batchGen1Options">Configuration of <see cref="Options.BatchImageGeneration1Options"/>.</param>
        /// <param name="batchGen2Options">Configuration of <see cref="Options.BatchImageGeneration2Options"/>.</param>
        /// <param name="marthaOptions">Configuration of <see cref="Options.MarthaOptions"/>.</param>
        /// <param name="storageOptions">Configuration of <see cref="Options.StorageOptions"/>.</param>
        /// <param name="batchImageNameOptions">Configuration of <see cref="Options.BatchImageNameOptions"/>.</param>
        /// <param name="batchNodesOptions">Configuration of <see cref="Options.BatchNodesOptions"/>.</param>
        /// <param name="batchSchedulingOptions">Configuration of <see cref="Options.BatchSchedulingOptions"/>.</param>
        /// <param name="azureProxy">Azure proxy <see cref="IAzureProxy"/>.</param>
        /// <param name="storageAccessProvider">Storage access provider <see cref="IStorageAccessProvider"/>.</param>
        /// <param name="quotaVerifier">Quota verifier <see cref="IBatchQuotaVerifier"/>.</param>
        /// <param name="skuInformationProvider">Sku information provider <see cref="IBatchSkuInformationProvider"/>.</param>
        /// <param name="containerRegistryProvider">Container registry information <see cref="ContainerRegistryProvider"/>.</param>
        /// <param name="poolFactory"><see cref="IBatchPool"/> factory.</param>
        /// <param name="allowedVmSizesService">Service to get allowed vm sizes.</param>
        /// <param name="taskExecutionScriptingManager"><see cref="TaskExecutionScriptingManager"/>.</param>
        public BatchScheduler(
            ILogger<BatchScheduler> logger,
            IOptions<Options.BatchImageGeneration1Options> batchGen1Options,
            IOptions<Options.BatchImageGeneration2Options> batchGen2Options,
            IOptions<Options.MarthaOptions> marthaOptions,
            IOptions<Options.StorageOptions> storageOptions,
            IOptions<Options.BatchImageNameOptions> batchImageNameOptions,
            IOptions<Options.BatchNodesOptions> batchNodesOptions,
            IOptions<Options.BatchSchedulingOptions> batchSchedulingOptions,
            IAzureProxy azureProxy,
            IStorageAccessProvider storageAccessProvider,
            IBatchQuotaVerifier quotaVerifier,
            IBatchSkuInformationProvider skuInformationProvider,
            ContainerRegistryProvider containerRegistryProvider,
            Func<IBatchPool> poolFactory,
            IAllowedVmSizesService allowedVmSizesService,
            TaskExecutionScriptingManager taskExecutionScriptingManager)
        {
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(azureProxy);
            ArgumentNullException.ThrowIfNull(storageAccessProvider);
            ArgumentNullException.ThrowIfNull(quotaVerifier);
            ArgumentNullException.ThrowIfNull(skuInformationProvider);
            ArgumentNullException.ThrowIfNull(containerRegistryProvider);
            ArgumentNullException.ThrowIfNull(poolFactory);
            ArgumentNullException.ThrowIfNull(taskExecutionScriptingManager);

            this.logger = logger;
            this.azureProxy = azureProxy;
            this.storageAccessProvider = storageAccessProvider;
            this.quotaVerifier = quotaVerifier;
            this.skuInformationProvider = skuInformationProvider;
            this.containerRegistryProvider = containerRegistryProvider;

            this.usePreemptibleVmsOnly = batchSchedulingOptions.Value.UsePreemptibleVmsOnly;
            this.batchNodesSubnetId = batchNodesOptions.Value.SubnetId;
            this.dockerInDockerImageName = batchImageNameOptions.Value.Docker;
            if (string.IsNullOrWhiteSpace(this.dockerInDockerImageName)) { this.dockerInDockerImageName = Options.BatchImageNameOptions.DefaultDocker; }
            this.cromwellDrsLocalizerImageName = marthaOptions.Value.CromwellDrsLocalizer;
            if (string.IsNullOrWhiteSpace(this.cromwellDrsLocalizerImageName)) { this.cromwellDrsLocalizerImageName = Options.MarthaOptions.DefaultCromwellDrsLocalizer; }
            this.disableBatchNodesPublicIpAddress = batchNodesOptions.Value.DisablePublicIpAddress;
            this.poolLifetime = TimeSpan.FromDays(batchSchedulingOptions.Value.PoolRotationForcedDays == 0 ? Options.BatchSchedulingOptions.DefaultPoolRotationForcedDays : batchSchedulingOptions.Value.PoolRotationForcedDays);
            this.defaultStorageAccountName = storageOptions.Value.DefaultAccountName;
            logger.LogInformation(@"Default storage account: {DefaultStorageAccountName}", defaultStorageAccountName);
            this.globalStartTaskPath = StandardizeStartTaskPath(batchNodesOptions.Value.GlobalStartTask, this.defaultStorageAccountName);
            this.globalManagedIdentity = batchNodesOptions.Value.GlobalManagedIdentity;
            this.allowedVmSizesService = allowedVmSizesService;
            this.taskExecutionScriptingManager = taskExecutionScriptingManager;

            batchPoolFactory = poolFactory;
            batchPrefix = batchSchedulingOptions.Value.Prefix;
            logger.LogInformation("BatchPrefix: {BatchPrefix}", batchPrefix);
            File.ReadAllLines(Path.Combine(AppContext.BaseDirectory, "scripts/task-run.sh"));

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

            logger.LogInformation(@"usePreemptibleVmsOnly: {UsePreemptibleVmsOnly}", usePreemptibleVmsOnly);

            static bool tesTaskIsQueuedInitializingOrRunning(TesTask tesTask) => tesTask.State == TesState.QUEUEDEnum || tesTask.State == TesState.INITIALIZINGEnum || tesTask.State == TesState.RUNNINGEnum;
            static bool tesTaskIsInitializingOrRunning(TesTask tesTask) => tesTask.State == TesState.INITIALIZINGEnum || tesTask.State == TesState.RUNNINGEnum;
            static bool tesTaskIsQueuedOrInitializing(TesTask tesTask) => tesTask.State == TesState.QUEUEDEnum || tesTask.State == TesState.INITIALIZINGEnum;
            static bool tesTaskIsQueued(TesTask tesTask) => tesTask.State == TesState.QUEUEDEnum;
            static bool tesTaskCancellationRequested(TesTask tesTask) => tesTask.State == TesState.CANCELINGEnum;
            static bool tesTaskDeletionReady(TesTask tesTask) => tesTask.IsTaskDeletionRequired;

            var setTaskStateLock = new object();

            async Task<bool> SetTaskStateAndLog(TesTask tesTask, TesState newTaskState, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
            {
                {
                    var newData = System.Text.Json.JsonSerializer.Serialize(
                        batchInfo,
                        new System.Text.Json.JsonSerializerOptions()
                        {
                            DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingDefault,
                            Converters = { new System.Text.Json.Serialization.JsonStringEnumConverter(System.Text.Json.JsonNamingPolicy.CamelCase) }
                        });

                    if ("{}".Equals(newData) && newTaskState == tesTask.State)
                    {
                        logger.LogDebug(@"For task {TesTask} there's nothing to change.", tesTask.Id);
                        return false;
                    }

                    logger.LogDebug(@"Setting task {TesTask} with metadata {Metadata}.", tesTask.Id, newData);
                }

                var (batchNodeMetrics, taskStartTime, taskEndTime, cromwellRcCode) = newTaskState == TesState.COMPLETEEnum
                    ? await GetBatchNodeMetricsAndCromwellResultCodeAsync(tesTask, cancellationToken)
                : default;

                lock (setTaskStateLock)
                {
                    tesTask.State = newTaskState;

                    var tesTaskLog = tesTask.GetOrAddTesTaskLog();
                    var tesTaskExecutorLog = tesTaskLog.GetOrAddExecutorLog();

                    if (tesTaskLog.Outputs is not null && !(batchInfo.OutputFileLogs?.Any() ?? true))
                    {
                        tesTaskLog.Outputs = batchInfo.OutputFileLogs?.Select(
                            entry => new Tes.Models.TesOutputFileLog
                            {
                                Path = entry.Path,
                                SizeBytes = $"{entry.Size}",
                                Url = entry.Url.AbsoluteUri
                            }).ToList();
                    }

                    tesTaskLog.BatchNodeMetrics = batchNodeMetrics;
                    tesTaskLog.CromwellResultCode = cromwellRcCode;
                    tesTaskLog.EndTime ??= taskEndTime ?? batchInfo.BatchTaskEndTime;
                    tesTaskLog.StartTime ??= taskStartTime ?? batchInfo.BatchTaskStartTime;
                    tesTaskExecutorLog.StartTime ??= batchInfo.ExecutorStartTime;
                    tesTaskExecutorLog.EndTime ??= batchInfo.ExecutorEndTime;
                    tesTaskExecutorLog.ExitCode ??= batchInfo.ExecutorExitCode;

                    // Only accurate when the task completes successfully, otherwise it's the Batch time as reported from Batch
                    // TODO this could get large; why?
                    //var timefromCoAScriptCompletionToBatchTaskDetectedComplete = tesTaskLog.EndTime - tesTaskExecutorLog.EndTime;

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

                    if (batchInfo.Failure is not null)
                    {
                        tesTask.SetFailureReason(batchInfo.Failure.Reason);

                        if (batchInfo.Failure.SystemLogs is not null)
                        {
                            tesTask.AddToSystemLog(batchInfo.Failure.SystemLogs);
                        }
                        else if (!string.IsNullOrWhiteSpace(batchInfo.AlternateSystemLogItem))
                        {
                            tesTask.AddToSystemLog(new[] { batchInfo.AlternateSystemLogItem });
                        }
                    }
                }

                if (!tesTask.IsActiveState())
                {
                    logger.LogDebug(@"Uploading completed {TesTask}.", tesTask.Id);
                    await taskExecutionScriptingManager.TryUploadServerTesTask(tesTask, "server-tes-task-completed.json", cancellationToken);
                }

                return true;
            }

            async Task<bool> SetTaskCompleted(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
            {
                await azureProxy.DeleteBatchTaskAsync(tesTask.Id, tesTask.PoolId, cancellationToken);
                return await SetTaskStateAndLog(tesTask, TesState.COMPLETEEnum, batchInfo, cancellationToken);
            }

            async Task<bool> SetTaskExecutorError(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
            {
                await azureProxy.DeleteBatchTaskAsync(tesTask.Id, tesTask.PoolId, cancellationToken);
                return await SetTaskStateAndLog(tesTask, TesState.EXECUTORERROREnum, batchInfo, cancellationToken);
            }

            async Task<bool> SetTaskSystemError(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
            {
                await azureProxy.DeleteBatchTaskAsync(tesTask.Id, tesTask.PoolId, cancellationToken);
                return await SetTaskStateAndLog(tesTask, TesState.SYSTEMERROREnum, batchInfo, cancellationToken);
            }

            async Task<bool> SetTaskStateAfterFailureAsync(TesTask tesTask, TesState newTaskState, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
            {
                await azureProxy.DeleteBatchTaskAsync(tesTask.Id, tesTask.PoolId, cancellationToken);
                return await SetTaskStateAndLog(tesTask, newTaskState, batchInfo, cancellationToken);
            }

            Task<bool> RequeueTaskAfterFailureAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
                => ++tesTask.ErrorCount > 3
                    ? AddSystemLogAndSetTaskExecutorErrorAsync(tesTask, batchInfo, "System Error: Retry count exceeded.", cancellationToken)
                    : SetTaskStateAfterFailureAsync(tesTask, TesState.QUEUEDEnum, batchInfo, cancellationToken);

            Task<bool> AddSystemLogAndSetTaskExecutorErrorAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo, string additionalSystemLogItem, CancellationToken cancellationToken)
            {
                return SetTaskExecutorError(tesTask, new(batchInfo, additionalSystemLogItem), cancellationToken);
            }

            Task<bool> HandlePreemptedNodeAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
            {
                logger.LogInformation("The TesTask {TesTask}'s node was preempted. It will be automatically rescheduled.", tesTask.Id);
                tesTask.State = TesState.INITIALIZINGEnum;
                return Task.FromResult(false);
            }

            Task<bool> HandleInfoUpdate(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
            {
                return SetTaskStateAndLog(tesTask, tesTask.State, batchInfo, cancellationToken);
            }

            tesTaskStateTransitions = new List<TesTaskStateTransition>()
            {
                new TesTaskStateTransition(tesTaskDeletionReady, batchTaskState: null, alternateSystemLogItem: null, (tesTask, _, ct) => DeleteCancelledTaskAsync(tesTask, ct)),
                new TesTaskStateTransition(tesTaskCancellationRequested, batchTaskState: null, alternateSystemLogItem: null, (tesTask, _, ct) => TerminateBatchTaskAsync(tesTask, ct)),
                //new TesTaskStateTransition(tesTaskIsQueued, BatchTaskState.JobNotFound, alternateSystemLogItem: null, (tesTask, _, ct) => AddBatchTaskAsync(tesTask, ct)),
                //new TesTaskStateTransition(tesTaskIsQueued, BatchTaskState.MissingBatchTask, alternateSystemLogItem: null, (tesTask, _, ct) => AddBatchTaskAsync(tesTask, ct)),
                new TesTaskStateTransition(tesTaskIsQueued, AzureBatchTaskState.TaskState.Initializing, alternateSystemLogItem: null, (tesTask, _) => { tesTask.State = TesState.INITIALIZINGEnum; return true; }),
                new TesTaskStateTransition(tesTaskIsQueuedOrInitializing, AzureBatchTaskState.TaskState.NodeAllocationFailed, alternateSystemLogItem: null, RequeueTaskAfterFailureAsync),
                new TesTaskStateTransition(tesTaskIsQueuedInitializingOrRunning, AzureBatchTaskState.TaskState.Running, alternateSystemLogItem: null, (tesTask, info, ct) => SetTaskStateAndLog(tesTask, TesState.RUNNINGEnum, info, ct)),
                //new TesTaskStateTransition(tesTaskIsQueuedInitializingOrRunning, AzureBatchTaskState.TaskState.MoreThanOneActiveJobOrTaskFound, BatchTaskState.MoreThanOneActiveJobOrTaskFound.ToString(), DeleteBatchJobAndSetTaskSystemErrorAsync),
                new TesTaskStateTransition(tesTaskIsQueuedInitializingOrRunning, AzureBatchTaskState.TaskState.CompletedSuccessfully, alternateSystemLogItem: null, SetTaskCompleted),
                new TesTaskStateTransition(tesTaskIsQueuedInitializingOrRunning, AzureBatchTaskState.TaskState.CompletedWithErrors, "Please open an issue. There should have been an error reported here.", SetTaskExecutorError),
                //new TesTaskStateTransition(tesTaskIsQueuedInitializingOrRunning, AzureBatchTaskState.TaskState.ActiveJobWithMissingAutoPool, alternateSystemLogItem: null, DeleteBatchJobAndRequeueTaskAsync),
                new TesTaskStateTransition(tesTaskIsQueuedInitializingOrRunning, AzureBatchTaskState.TaskState.NodeFailedDuringStartupOrExecution, "Please open an issue. There should have been an error reported here.", SetTaskSystemError),
                new TesTaskStateTransition(tesTaskIsQueuedInitializingOrRunning, AzureBatchTaskState.TaskState.NodeUnusable, "Please open an issue. There should have been an error reported here.", SetTaskExecutorError),
                //new TesTaskStateTransition(tesTaskIsInitializingOrRunning, AzureBatchTaskState.TaskState.JobNotFound, BatchTaskState.JobNotFound.ToString(), SetTaskSystemError),
                //new TesTaskStateTransition(tesTaskIsInitializingOrRunning, AzureBatchTaskState.TaskState.MissingBatchTask, BatchTaskState.MissingBatchTask.ToString(), DeleteBatchJobAndSetTaskSystemErrorAsync),
                new TesTaskStateTransition(tesTaskIsInitializingOrRunning, AzureBatchTaskState.TaskState.NodePreempted, alternateSystemLogItem: null, HandlePreemptedNodeAsync),
                new TesTaskStateTransition(tesTaskIsQueuedInitializingOrRunning, AzureBatchTaskState.TaskState.NodeFilesUploadOrDownloadFailed, alternateSystemLogItem: null, HandleInfoUpdate),
                new TesTaskStateTransition(condition: null, AzureBatchTaskState.TaskState.InfoUpdate, alternateSystemLogItem: null, HandleInfoUpdate),
            }.AsReadOnly();
        }

        private async Task<bool> DeleteCancelledTaskAsync(TesTask tesTask, CancellationToken cancellationToken)
        {
            // https://learn.microsoft.com/azure/batch/best-practices#manage-task-lifetime
            var mins10 = TimeSpan.FromMinutes(10);
            var now = DateTimeOffset.UtcNow;

            if (!tesTask.Logs.Any(l => now - l.StartTime > mins10))
            {
                return false;
            }

            await azureProxy.DeleteBatchTaskAsync(tesTask.Id, tesTask.PoolId, cancellationToken);
            tesTask.IsTaskDeletionRequired = false;
            await taskExecutionScriptingManager.TryUploadServerTesTask(tesTask, "server-tes-task-completed.json", cancellationToken);
            return true;
        }

        private async Task<bool> TerminateBatchTaskAsync(TesTask tesTask, CancellationToken cancellationToken)
        {
            try
            {
                await azureProxy.TerminateBatchTaskAsync(tesTask.Id, tesTask.PoolId, cancellationToken);
                tesTask.IsTaskDeletionRequired = true;
                tesTask.State = TesState.CANCELEDEnum;
                return true;
            }
            //TODO: catch exception returned if the task was already completed.
            catch (Exception exc)
            {
                logger.LogError(exc, "Exception terminating batch task with tesTask.Id: {TesTaskId}", tesTask?.Id);
                throw;
            }
        }

        /// <summary>
        /// Creates a wget command to robustly download a file
        /// </summary>
        /// <param name="urlToDownload">URL to download</param>
        /// <param name="localFilePathDownloadLocation">Filename for the output file</param>
        /// <param name="setExecutable">Whether the file should be made executable or not</param>
        /// <returns>The command to execute</returns>
        private string CreateWgetDownloadCommand(string urlToDownload, string localFilePathDownloadLocation, bool setExecutable = false)
        {
            var command = $"wget --no-verbose --https-only --timeout=20 --waitretry=1 --tries=9 --retry-connrefused --continue -O {localFilePathDownloadLocation} '{urlToDownload}'";

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
                    var forceRemove = !string.IsNullOrWhiteSpace(globalManagedIdentity) && !(cloudPool.Identity?.UserAssignedIdentities?.Any(id => globalManagedIdentity.Equals(id.ResourceId, StringComparison.OrdinalIgnoreCase)) ?? false);
                    var batchPool = batchPoolFactory();
                    await batchPool.AssignPoolAsync(cloudPool, forceRemove, cancellationToken);
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, "When retrieving previously created batch pools and jobs, there were one or more failures when trying to access batch pool {PoolId} or its associated job.", cloudPool.Id);
                }
            }
        }

        /// <inheritdoc/>
        public async Task UploadTaskRunnerIfNeeded(CancellationToken cancellationToken)
        {
            var blobUri = new Uri(await storageAccessProvider.GetInternalTesBlobUrlAsync(NodeTaskRunnerFilename, storageAccessProvider.BlobPermissionsWithWrite, cancellationToken));
            var blobProperties = await azureProxy.GetBlobPropertiesAsync(blobUri, cancellationToken);
            if (!(await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, $"scripts/{NodeTaskRunnerMD5HashFilename}"), cancellationToken)).Trim().Equals(blobProperties?.ContentMD5, StringComparison.OrdinalIgnoreCase))
            {
                await azureProxy.UploadBlobFromFileAsync(blobUri, $"scripts/{NodeTaskRunnerFilename}", cancellationToken);
            }
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<TesTaskTask<bool>> ProcessTesTaskBatchStatesAsync(IEnumerable<TesTask> tesTasks, AzureBatchTaskState[] taskStates, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(tesTasks);
            ArgumentNullException.ThrowIfNull(taskStates);

            return taskStates.Zip(tesTasks, (TaskState, TesTask) => (TaskState, TesTask))
                .Where(entry => entry.TesTask?.IsActiveState() ?? false) // Removes already terminal (and null) TesTasks from being further processed.
                .Select(entry => new TesTaskTask<bool>(WrapHandleTesTaskTransitionAsync(entry.TesTask, entry.TaskState, cancellationToken), entry.TesTask))
                .WhenEach(cancellationToken, tesTaskTask => tesTaskTask.Task);

            async Task<bool> WrapHandleTesTaskTransitionAsync(TesTask tesTask, AzureBatchTaskState azureBatchTaskState, CancellationToken cancellationToken)
                => await HandleTesTaskTransitionAsync(tesTask, azureBatchTaskState, cancellationToken);

            //Task<bool> WrapHandleTesTaskTransitionAsync(TesTask tesTask, AzureBatchTaskState azureBatchTaskState, CancellationToken cancellationToken)
            //    => Task.Run(async () => await HandleTesTaskTransitionAsync(tesTask, azureBatchTaskState, cancellationToken));
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

        private static string GetCromwellExecutionDirectoryPathAsExecutionContainerPath(TesTask task)
        {
            return task.Inputs?.FirstOrDefault(IsCromwellCommandScript)?.Path;
        }

        private string GetStorageUploadPath(TesTask task)
        {
            return task.Resources?.ContainsBackendParameterValue(TesResources.SupportedBackendParameters.internal_path_prefix) ?? false
                ? $"{defaultStorageAccountName}/{task.Resources.GetBackendParameterValue(TesResources.SupportedBackendParameters.internal_path_prefix).Trim('/')}"
                : $"{defaultStorageAccountName}{TesExecutionsPathPrefix}/{task.Id}";
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

        /// <inheritdoc/>
        public async IAsyncEnumerable<TesTaskTask<bool>> ProcessQueuedTesTasksAsync(TesTask[] tesTasks, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var tasksMetadataByPoolKey = new Dictionary<string, List<(TesTask TesTask, VirtualMachineInformation VirtualMachineInfo, (BatchModels.ContainerConfiguration ContainerConfiguration, (bool ExecutorImage, bool DockerInDockerImage, bool CromwellDrsImage) IsPublic) ContainerMetadata, IEnumerable<string> Identities, string PoolDisplayName)>>();
            var poolKeyByTaskIds = new Dictionary<string, string>(); // Reverse lookup of 'tasksMetadataByPoolKey'

            {
                var tasks = tesTasks.ToList(); // List of tasks that will make it to the next round.

                // Determine how many nodes in each pool we might need for this group.
                foreach (var tesTask in tesTasks) // TODO: Consider parallelizing this foreach loop.
                {
                    Task<bool> quickResult = default; // fast exit enabler
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
                        var virtualMachineInfo = await GetVmSizeAsync(tesTask, cancellationToken);
                        var containerMetadata = await GetContainerConfigurationIfNeededAsync(tesTask, cancellationToken);
                        (poolKey, var displayName) = GetPoolKey(tesTask, virtualMachineInfo, containerMetadata.ContainerConfiguration, identities, cancellationToken);
                        await quotaVerifier.CheckBatchAccountQuotasAsync(virtualMachineInfo, needPoolOrJobQuotaCheck: !IsPoolAvailable(poolKey), cancellationToken: cancellationToken);

                        if (tasksMetadataByPoolKey.TryGetValue(poolKey, out var resource))
                        {
                            resource.Add((tesTask, virtualMachineInfo, containerMetadata, identities, displayName));
                        }
                        else
                        {
                            tasksMetadataByPoolKey.Add(poolKey, new() { (tesTask, virtualMachineInfo, containerMetadata, identities, displayName) });
                        }

                        poolKeyByTaskIds.Add(tesTask.Id, poolKey);
                    }
                    catch (Exception ex)
                    {
                        quickResult = HandleException(ex, poolKey, tesTask);
                    }

                    if (quickResult is not null)
                    {
                        tasks.Remove(tesTask);
                        yield return new(quickResult, tesTask);
                    }
                }

                // Remove already returned tasks from the dictionary
                tasksMetadataByPoolKey = tasksMetadataByPoolKey
                    .Select(p => (p.Key, Value: p.Value.Where(v => tasks.Contains(v.TesTask)).ToList())) // keep only tasks that remain in the 'tasks' variable
                    .Where(t => t.Value.Count != 0) // Remove any now empty pool keys
                    .ToDictionary(p => p.Key, p => p.Value);
            }

            // Determine how many nodes in each new pool we might need for this group.
            var neededPoolNodesByPoolKey = tasksMetadataByPoolKey.ToDictionary(t => t.Key, t => t.Value.Count);

            {
                // Determine how many new pools/jobs we will need for this batch
                var requiredNewPools = neededPoolNodesByPoolKey.Where(t => !IsPoolAvailable(t.Key)).Count();

                // Revisit pool/job quotas (the above loop already dealt with the possiblility of needing just one more pool/job).
                // This will remove pool keys we cannot accomodate due to quota, along with all of their associated tasks, from being queued into Batch.
                if (requiredNewPools > 1)
                {
                    var (excess, exception) = await quotaVerifier.CheckBatchAccountPoolAndJobQuotasAsync(requiredNewPools, cancellationToken);
                    var initial = tasksMetadataByPoolKey.Count - 1;
                    var final = initial - excess;

                    for (var i = initial; i > final; --i)
                    {
                        var key = tasksMetadataByPoolKey.Keys.ElementAt(i);
                        if (tasksMetadataByPoolKey.Remove(key, out var listOfTaskMetadata))
                        {
                            foreach (var (task, _, _, _, _) in listOfTaskMetadata)
                            {
                                yield return new(HandleException(exception, key, task), task);
                            }
                        }
                    }
                }
            }

            // Obtain assigned pool and create and assign the cloudtask for each task.
            foreach (var (tesTask, virtualMachineInfo, containerMetadata, identities, displayName) in tasksMetadataByPoolKey.Values.SelectMany(e => e)) // TODO: Consider parallelizing this foreach loop. Would require making GetOrAddPoolAsync multi-threaded safe.
            {
                Task<bool> quickResult = default; // fast exit enabler
                var poolKey = poolKeyByTaskIds[tesTask.Id];

                try
                {
                    string poolId = null;
                    var tesTaskLog = tesTask.AddTesTaskLog();
                    tesTaskLog.VirtualMachineInfo = virtualMachineInfo;
                    poolId = (await GetOrAddPoolAsync(
                        key: poolKey,
                        isPreemptable: virtualMachineInfo.LowPriority,
                        modelPoolFactory: async (id, ct) => await GetPoolSpecification(
                            name: id,
                            displayName: displayName,
                            poolIdentity: GetBatchPoolIdentity(identities.ToArray()),
                            vmSize: virtualMachineInfo.VmSize,
                            autoscaled: true,
                            preemptable: virtualMachineInfo.LowPriority,
                            initialTarget: neededPoolNodesByPoolKey[poolKey],
                            nodeInfo: (virtualMachineInfo.HyperVGenerations?.Contains("V2")).GetValueOrDefault() ? gen2BatchNodeInfo : gen1BatchNodeInfo,
                            containerConfiguration: containerMetadata.ContainerConfiguration,
                            encryptionAtHostSupported: virtualMachineInfo.EncryptionAtHostSupported,
                            cancellationToken: ct),
                        cancellationToken: cancellationToken)).Id;

                    var cloudTaskId = $"{tesTask.Id}-{tesTask.Logs.Count}";
                    tesTask.PoolId = poolId;
                    var cloudTask = await ConvertTesTaskToBatchTaskUsingRunnerAsync(cloudTaskId, tesTask, cancellationToken);

                    logger.LogInformation(@"Creating batch task for TES task {TesTaskId}. Using VM size {VmSize}.", tesTask.Id, virtualMachineInfo.VmSize);
                    await azureProxy.AddBatchTaskAsync(tesTask.Id, cloudTask, poolId, cancellationToken);

                    tesTaskLog.StartTime = DateTimeOffset.UtcNow;
                    tesTask.State = TesState.INITIALIZINGEnum;
                }
                catch (AggregateException aggregateException)
                {
                    var exceptions = new List<Exception>();

                    foreach (var partResult in aggregateException.Flatten().InnerExceptions.Select(ex => HandleException(ex, poolKey, tesTask)))
                    {
                        if (partResult.IsFaulted)
                        {
                            exceptions.Add(partResult.Exception);
                        }
                    }

                    quickResult = exceptions.Count == 0
                        ? Task.FromResult(true)
                        : Task.FromException<bool>(new AggregateException(exceptions));
                }
                catch (Exception exception)
                {
                    quickResult = HandleException(exception, poolKey, tesTask);
                }

                if (quickResult is not null)
                {
                    yield return new(quickResult, tesTask);
                }
                else
                {
                    yield return new(Task.FromResult(true), tesTask);
                }
            }

            Task<bool> HandleException(Exception exception, string poolKey, TesTask tesTask)
            {
                switch (exception)
                {
                    case AzureBatchPoolCreationException azureBatchPoolCreationException:
                        if (!azureBatchPoolCreationException.IsTimeout && !azureBatchPoolCreationException.IsJobQuota && !azureBatchPoolCreationException.IsPoolQuota && azureBatchPoolCreationException.InnerException is not null)
                        {
                            return HandleException(azureBatchPoolCreationException.InnerException, poolKey, tesTask);
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
                        tesTask.State = TesState.SYSTEMERROREnum;
                        tesTask.SetFailureReason("UnknownError", $"{exception?.GetType().FullName}: {exception?.Message}", exception?.StackTrace);
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
                ?.ActionAsync(tesTask, azureBatchTaskState, cancellationToken) ?? ValueTask.FromResult(false);

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
                UserIdentity = new UserIdentity(new AutoUserSpecification(elevationLevel: ElevationLevel.Admin, scope: AutoUserScope.Pool)),
            };

            return cloudTask;
        }

        private async Task<NodeTaskConversionOptions> GetNodeTaskConversionOptionsAsync(TesTask task, CancellationToken cancellationToken)
        {
            var nodeTaskCreationOptions = new NodeTaskConversionOptions(
                DefaultStorageAccountName: defaultStorageAccountName,
                AdditionalInputs: await GetAdditionalCromwellInputsAsync(task, cancellationToken),
                NodeManagedIdentityResourceId: GetNodeManagedIdentityResourceId(task)
            );
            return nodeTaskCreationOptions;
        }

        private string GetNodeManagedIdentityResourceId(TesTask task)
        {
            var resourceId =
                task.Resources?.GetBackendParameterValue(TesResources.SupportedBackendParameters
                    .workflow_execution_identity);

            if (!string.IsNullOrEmpty(resourceId))
            {
                return resourceId;
            }

            return globalManagedIdentity;
        }

        private async Task<IList<TesInput>> GetAdditionalCromwellInputsAsync(TesTask task, CancellationToken cancellationToken)
        {
            var cromwellExecutionDirectoryUrl = GetCromwellExecutionDirectoryPathAsUrl(task);

            // TODO: Cromwell bug: Cromwell command write_tsv() generates a file in the execution directory, for example execution/write_tsv_3922310b441805fc43d52f293623efbc.tmp. These are not passed on to TES inputs.
            // WORKAROUND: Get the list of files in the execution directory and add them to task inputs.
            // TODO: Verify whether this workaround is still needed.
            var additionalInputs = new List<TesInput>();

            if (cromwellExecutionDirectoryUrl is not null)
            {
                additionalInputs =
                    await GetExistingBlobsInCromwellStorageLocationAsTesInputsAsync(task, cromwellExecutionDirectoryUrl,
                        cancellationToken);
            }

            return additionalInputs;
        }

        private async Task<List<TesInput>> GetExistingBlobsInCromwellStorageLocationAsTesInputsAsync(TesTask task,
            string cromwellExecutionDirectoryUrl, CancellationToken cancellationToken)
        {
            List<TesInput> additionalInputFiles = default;
            var scriptPath = GetCromwellExecutionDirectoryPathAsExecutionContainerPath(task);

            if (!Uri.TryCreate(cromwellExecutionDirectoryUrl, UriKind.Absolute, out _))
            {
                cromwellExecutionDirectoryUrl = $"/{cromwellExecutionDirectoryUrl}";
            }

            var executionDirectoryUriString = await storageAccessProvider.MapLocalPathToSasUrlAsync(cromwellExecutionDirectoryUrl,
                storageAccessProvider.DefaultContainerPermissions, cancellationToken);

            var executionDirectoryUri = string.IsNullOrEmpty(executionDirectoryUriString) ? null : new Uri(executionDirectoryUriString);

            if (executionDirectoryUri is not null)
            {
                var executionDirectoryBlobName = new Azure.Storage.Blobs.BlobUriBuilder(executionDirectoryUri).BlobName;
                var startOfBlobNameIndex = scriptPath.IndexOf(executionDirectoryBlobName, StringComparison.OrdinalIgnoreCase);
                var pathBlobPrefix = scriptPath[..startOfBlobNameIndex];

                var blobsInExecutionDirectory =
                    await azureProxy.ListBlobsAsync(executionDirectoryUri, cancellationToken)
                        .Select(info => (Path: $"{pathBlobPrefix}{info.BlobName}", Uri: info.BlobUri))
                        .ToListAsync(cancellationToken);

                var scriptBlob =
                    blobsInExecutionDirectory.FirstOrDefault(b => scriptPath.Equals(b.Path, StringComparison.OrdinalIgnoreCase));

                var expectedPathParts = scriptPath.Split('/').Length;

                additionalInputFiles = blobsInExecutionDirectory
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

            return additionalInputFiles ?? new();
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

            if (inputFile.Type == TesFileType.DIRECTORYEnum)
            {
                throw new TesException("InvalidInputFilePath", "Directory input is not supported.");
            }
        }

        /// <summary>
        /// Constructs a universal Azure Start Task instance if needed
        /// </summary>
        /// <param name="machineConfiguration">A <see cref="VirtualMachineConfiguration"/> describing the OS of the pool's nodes.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        /// <remarks>This method also mitigates errors associated with docker daemons that are not configured to place their filesystem assets on the data drive.</remarks>
        private async Task<BatchModels.StartTask> StartTaskIfNeeded(BatchModels.VirtualMachineConfiguration machineConfiguration, CancellationToken cancellationToken)
        {
            var globalStartTaskConfigured = !string.IsNullOrWhiteSpace(globalStartTaskPath);

            var startTaskSasUrl = globalStartTaskConfigured
                ? await storageAccessProvider.MapLocalPathToSasUrlAsync(globalStartTaskPath, storageAccessProvider.DefaultBlobPermissions, cancellationToken, sasTokenDuration: BatchPoolService.RunInterval.Multiply(2).Add(poolLifetime).Add(TimeSpan.FromMinutes(15)))
                : default;

            if (startTaskSasUrl is not null)
            {
                if (!await azureProxy.BlobExistsAsync(new Uri(startTaskSasUrl), cancellationToken))
                {
                    startTaskSasUrl = default;
                    globalStartTaskConfigured = false;
                }
            }
            else
            {
                globalStartTaskConfigured = false;
            }

            // https://learn.microsoft.com/azure/batch/batch-docker-container-workloads#linux-support
            var dockerConfigured = machineConfiguration.ImageReference.Publisher.Equals("microsoft-azure-batch", StringComparison.InvariantCultureIgnoreCase)
                && (machineConfiguration.ImageReference.Offer.StartsWith("ubuntu-server-container", StringComparison.InvariantCultureIgnoreCase) || machineConfiguration.ImageReference.Offer.StartsWith("centos-container", StringComparison.InvariantCultureIgnoreCase));

            var dockerConfigCmdLine = new Func<string>(() =>
            {
                var commandLine = new StringBuilder();
                commandLine.Append(@"/usr/bin/bash -c 'trap ""echo Error trapped; exit 0"" ERR; sudo touch tmp2.json && (sudo cp /etc/docker/daemon.json tmp1.json || sudo echo {} > tmp1.json) && sudo chmod a+w tmp?.json && if fgrep ""$(dirname ""$(dirname ""$AZ_BATCH_NODE_ROOT_DIR"")"")/docker"" tmp1.json; then echo grep ""found docker path""; elif [ $? -eq 1 ]; then ");

                commandLine.Append(machineConfiguration.NodeAgentSkuId switch
                {
                    var s when s.StartsWith("batch.node.ubuntu ") => "sudo apt-get install -y jq",
                    var s when s.StartsWith("batch.node.centos ") => "sudo yum install epel-release -y && sudo yum update -y && sudo yum install -y jq wget",
                    _ => throw new InvalidOperationException($"Unrecognized OS. Please send open an issue @ 'https://github.com/microsoft/ga4gh-tes/issues' with this message: ({machineConfiguration.NodeAgentSkuId})")
                });

                commandLine.Append(@" && jq \.\[\""data-root\""\]=\""""$(dirname ""$(dirname ""$AZ_BATCH_NODE_ROOT_DIR"")"")/docker""\"" tmp1.json >> tmp2.json && sudo cp tmp2.json /etc/docker/daemon.json && sudo chmod 644 /etc/docker/daemon.json && sudo systemctl restart docker && echo ""updated docker data-root""; else (echo ""grep failed"" || exit 1); fi'");

                return commandLine.ToString();
            });

            // Note that this has an embedded ')'. That is to faciliate merging with dockerConfigCmdLine.
            var globalStartTaskCmdLine = new Func<string>(() => $"{CreateWgetDownloadCommand(startTaskSasUrl, StartTaskScriptFilename, setExecutable: true)}) && ./{StartTaskScriptFilename}");

            BatchModels.StartTask startTask = new()
            {
                UserIdentity = new BatchModels.UserIdentity(autoUser: new BatchModels.AutoUserSpecification(elevationLevel: BatchModels.ElevationLevel.Admin, scope: BatchModels.AutoUserScope.Pool)),
                CommandLine = (!dockerConfigured, globalStartTaskConfigured) switch
                {
                    // Both start tasks are required. Note that dockerConfigCmdLine must be prefixed with an '(' which is closed inside of globalStartTaskCmdLine.
                    (true, true) => $"({dockerConfigCmdLine()} && {globalStartTaskCmdLine()}",

                    // Only globalStartTaskCmdLine is required. Note that it contains an embedded ')' so the shell starting '(' must be provided.
                    (false, true) => $"({globalStartTaskCmdLine()}",

                    // Only dockerConfigCmdLine is required. No additional subshell is needed.
                    (true, false) => dockerConfigCmdLine(),

                    // No start task is needed.
                    _ => string.Empty,
                },
            };

            return string.IsNullOrWhiteSpace(startTask.CommandLine) ? default : startTask;
        }

        /// <summary>
        /// Constructs an Azure Batch Container Configuration instance
        /// </summary>
        /// <param name="tesTask">The <see cref="TesTask"/> to schedule on Azure Batch</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        // TODO: remove this as soon as the node runner can authenticate to container registries
        private async ValueTask<(BatchModels.ContainerConfiguration ContainerConfiguration, (bool ExecutorImage, bool DockerInDockerImage, bool CromwellDrsImage) IsPublic)> GetContainerConfigurationIfNeededAsync(TesTask tesTask, CancellationToken cancellationToken)
        {
            var drsImageNeeded = tesTask.Inputs?.Any(i => i?.Url?.StartsWith("drs://") ?? false) ?? false;
            // TODO: Support for multiple executors. Cromwell has single executor per task.
            var executorImage = tesTask.Executors.First().Image;

            var dockerInDockerIsPublic = true;
            var executorImageIsPublic = containerRegistryProvider.IsImagePublic(executorImage);
            var cromwellDrsIsPublic = !drsImageNeeded || containerRegistryProvider.IsImagePublic(cromwellDrsLocalizerImageName);

            BatchModels.ContainerConfiguration result = default;

            if (!executorImageIsPublic || !cromwellDrsIsPublic)
            {
                var neededImages = new List<string> { executorImage, dockerInDockerImageName };
                if (drsImageNeeded)
                {
                    neededImages.Add(cromwellDrsLocalizerImageName);
                }

                // Download private images at node startup, since those cannot be downloaded in the main task that runs multiple containers.
                // Doing this also requires that the main task runs inside a container, hence downloading the "docker" image (contains docker client) as well.
                result = new BatchModels.ContainerConfiguration { ContainerImageNames = neededImages, ContainerRegistries = new List<BatchModels.ContainerRegistry>() };

                if (!executorImageIsPublic)
                {
                    _ = await AddRegistryIfNeeded(executorImage);
                }

                if (!cromwellDrsIsPublic)
                {
                    _ = await AddRegistryIfNeeded(cromwellDrsLocalizerImageName);
                }

                if (result.ContainerRegistries.Count != 0)
                {
                    dockerInDockerIsPublic = await AddRegistryIfNeeded(dockerInDockerImageName);
                }
            }

            return result is null || result.ContainerRegistries.Count == 0 ? (default, (true, true, true)) : (result, (executorImageIsPublic, dockerInDockerIsPublic, cromwellDrsIsPublic));

            async ValueTask<bool> AddRegistryIfNeeded(string imageName)
            {
                var containerRegistryInfo = await containerRegistryProvider.GetContainerRegistryInfoAsync(imageName, cancellationToken);

                if (containerRegistryInfo is not null && !result.ContainerRegistries.Any(registry => registry.RegistryServer == containerRegistryInfo.RegistryServer))
                {
                    result.ContainerRegistries.Add(new(
                        userName: containerRegistryInfo.Username,
                        registryServer: containerRegistryInfo.RegistryServer,
                        password: containerRegistryInfo.Password));

                    return true;
                }

                return false;
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
        /// Generate the <see cref="BatchModels.Pool"/> for the needed pool.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="displayName"></param>
        /// <param name="poolIdentity"></param>
        /// <param name="vmSize"></param>
        /// <param name="autoscaled"></param>
        /// <param name="preemptable"></param>
        /// <param name="initialTarget"></param>
        /// <param name="nodeInfo"></param>
        /// <param name="containerConfiguration"></param>
        /// <param name="encryptionAtHostSupported">VM supports encryption at host.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>The specification for the pool.</returns>
        /// <remarks>
        /// Devs: Any changes to any properties set in this method will require corresponding changes to all classes implementing <see cref="Management.Batch.IBatchPoolManager"/> along with possibly any systems they call, with the likely exception of <seealso cref="Management.Batch.ArmBatchPoolManager"/>.
        /// </remarks>
        private async ValueTask<BatchModels.Pool> GetPoolSpecification(string name, string displayName, BatchModels.BatchPoolIdentity poolIdentity, string vmSize, bool autoscaled, bool preemptable, int initialTarget, BatchNodeInfo nodeInfo, BatchModels.ContainerConfiguration containerConfiguration, bool encryptionAtHostSupported, CancellationToken cancellationToken)
        {
            ValidateString(name, nameof(name), 64);
            ValidateString(displayName, nameof(displayName), 1024);

            var vmConfig = new BatchModels.VirtualMachineConfiguration(
                imageReference: new BatchModels.ImageReference(
                    publisher: nodeInfo.BatchImagePublisher,
                    offer: nodeInfo.BatchImageOffer,
                    sku: nodeInfo.BatchImageSku,
                    version: nodeInfo.BatchImageVersion),
                nodeAgentSkuId: nodeInfo.BatchNodeAgentSkuId)
            {
                ContainerConfiguration = containerConfiguration
            };

            if (encryptionAtHostSupported)
            {
                vmConfig.DiskEncryptionConfiguration = new BatchModels.DiskEncryptionConfiguration(
                    targets: new List<BatchModels.DiskEncryptionTarget> { BatchModels.DiskEncryptionTarget.OsDisk, BatchModels.DiskEncryptionTarget.TemporaryDisk }
                );
            }

            BatchModels.ScaleSettings scaleSettings = new();

            if (autoscaled)
            {
                scaleSettings.AutoScale = new(BatchPool.AutoPoolFormula(preemptable, initialTarget), BatchPool.AutoScaleEvaluationInterval);
            }
            else
            {
                scaleSettings.FixedScale = new(
                    resizeTimeout: TimeSpan.FromMinutes(30),
                    targetDedicatedNodes: preemptable == false ? initialTarget : 0,
                    targetLowPriorityNodes: preemptable == true ? initialTarget : 0,
                    nodeDeallocationOption: BatchModels.ComputeNodeDeallocationOption.TaskCompletion);
            }

            BatchModels.Pool poolSpec = new(name: name, displayName: displayName, identity: poolIdentity)
            {
                VmSize = vmSize,
                ScaleSettings = scaleSettings,
                DeploymentConfiguration = new(virtualMachineConfiguration: vmConfig),
                //ApplicationPackages = ,
                StartTask = await StartTaskIfNeeded(vmConfig, cancellationToken),
                TargetNodeCommunicationMode = BatchModels.NodeCommunicationMode.Simplified,
            };

            if (!string.IsNullOrEmpty(batchNodesSubnetId))
            {
                poolSpec.NetworkConfiguration = new()
                {
                    PublicIPAddressConfiguration = new BatchModels.PublicIPAddressConfiguration(disableBatchNodesPublicIpAddress ? BatchModels.IPAddressProvisioningType.NoPublicIPAddresses : BatchModels.IPAddressProvisioningType.BatchManaged),
                    SubnetId = batchNodesSubnetId
                };
            }

            return poolSpec;

            static void ValidateString(string value, string paramName, int maxLength)
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
            bool allowedVmSizesFilter(VirtualMachineInformation vm) => allowedVmSizes is null || !allowedVmSizes.Any() || allowedVmSizes.Contains(vm.VmSize, StringComparer.OrdinalIgnoreCase) || allowedVmSizes.Contains(vm.VmFamily, StringComparer.OrdinalIgnoreCase);

            var tesResources = tesTask.Resources;

            var previouslyFailedVmSizes = tesTask.Logs?
                .Where(log => log.FailureReason == AzureBatchTaskState.TaskState.NodeAllocationFailed.ToString() && log.VirtualMachineInfo?.VmSize is not null)
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
                noVmFoundMessage += $" The following VM sizes were excluded from consideration because of {AzureBatchTaskState.TaskState.NodeAllocationFailed} error(s) on previous attempts: {string.Join(", ", previouslyFailedVmSizes)}.";
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
                    cromwellRcContentPath = cromwellRcContentBuilder.ToString();
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

                var metricsContent = await storageAccessProvider.DownloadBlobAsync($"/{GetStorageUploadPath(tesTask)}/metrics.txt", cancellationToken);

                if (metricsContent is not null)
                {
                    try
                    {
                        var metrics = DelimitedTextToDictionary(metricsContent.Trim());

                        var diskSizeInGB = TryGetValueAsDouble(metrics, "DiskSizeInKiB", out var diskSizeInKiB) ? diskSizeInKiB / kiBInGB : (double?)null;
                        var diskUsedInGB = TryGetValueAsDouble(metrics, "DiskUsedInKiB", out var diskUsedInKiB) ? diskUsedInKiB / kiBInGB : (double?)null;

                        batchNodeMetrics = new Tes.Models.BatchNodeMetrics
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
                        logger.LogError($"Failed to parse metrics for task {tesTask.Id}. Error: {ex.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogError($"Failed to get batch node metrics for task {tesTask.Id}. Error: {ex.Message}");
            }

            return (batchNodeMetrics, taskStartTime, taskEndTime, cromwellRcCode);
        }

        private static Dictionary<string, string> DelimitedTextToDictionary(string text, string fieldDelimiter = "=", string rowDelimiter = "\n")
            => text.Split(rowDelimiter)
                .Select(line => { var parts = line.Split(fieldDelimiter); return new KeyValuePair<string, string>(parts[0], parts[1]); })
                .ToDictionary(kv => kv.Key, kv => kv.Value);


        /// <inheritdoc/>
        public async IAsyncEnumerable<RunnerEventsMessage> GetEventMessagesAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken, string @event)
        {
            const string eventsFolderName = "events";
            var prefix = eventsFolderName + "/";

            if (!string.IsNullOrWhiteSpace(@event))
            {
                prefix += @event + "/";
            }

            var tesInternalSegments = StorageAccountUrlSegments.Create(storageAccessProvider.GetInternalTesBlobUrlWithoutSasToken(string.Empty));
            var eventsStartIndex = (string.IsNullOrEmpty(tesInternalSegments.BlobName) ? string.Empty : (tesInternalSegments.BlobName + "/")).Length;
            var eventsEndIndex = eventsStartIndex + eventsFolderName.Length + 1;

            await foreach (var blobItem in azureProxy.ListBlobsWithTagsAsync(
                    new(await storageAccessProvider.GetInternalTesBlobUrlAsync(
                        string.Empty,
                        Azure.Storage.Sas.BlobSasPermissions.Read | Azure.Storage.Sas.BlobSasPermissions.Tag | Azure.Storage.Sas.BlobSasPermissions.List,
                        cancellationToken)),
                    prefix,
                    cancellationToken)
                .WithCancellation(cancellationToken))
            {
                if (blobItem.Tags.ContainsKey(RunnerEventsProcessor.ProcessedTag) || !blobItem.Tags.ContainsKey("task-id"))
                {
                    continue;
                }

                var blobUrl = await storageAccessProvider.GetInternalTesBlobUrlAsync(blobItem.Name[eventsStartIndex..], storageAccessProvider.BlobPermissionsWithWriteAndTag, cancellationToken);

                var pathFromEventName = blobItem.Name[eventsEndIndex..];
                var eventName = pathFromEventName[..pathFromEventName.IndexOf('/')];

                yield return new(new(blobUrl), blobItem.Tags, eventName);
            }
        }

        /// <summary>
        /// Class that captures how <see cref="TesTask"/> transitions from current state to the new state, given the current Batch task state and optional condition. 
        /// Transitions typically include an action that needs to run in order for the task to move to the new state.
        /// </summary>
        private class TesTaskStateTransition
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
                    tesTaskChanged = await AsyncAction(tesTask, combinedBatchTaskInfo, cancellationToken);
                }

                if (Action is not null)
                {
                    tesTaskChanged = Action(tesTask, combinedBatchTaskInfo);
                }

                return tesTaskChanged;
            }
        }

        private record CombinedBatchTaskInfo : AzureBatchTaskState
        {
            public CombinedBatchTaskInfo(CombinedBatchTaskInfo state, string additionalSystemLogItem)
                : base(state, additionalSystemLogItem)
            {
                AlternateSystemLogItem = state.AlternateSystemLogItem;
            }

            public CombinedBatchTaskInfo(AzureBatchTaskState state, string alternateSystemLogItem)
                : base(state)
            {
                AlternateSystemLogItem = alternateSystemLogItem;
            }

            public string AlternateSystemLogItem { get; }
        }
    }
}
