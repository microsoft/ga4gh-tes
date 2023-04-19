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
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Tes.Extensions;
using Tes.Models;
using TesApi.Web.Extensions;
using TesApi.Web.Management;
using TesApi.Web.Management.Models.Quotas;
using TesApi.Web.Storage;
using BatchModels = Microsoft.Azure.Management.Batch.Models;
using TesException = Tes.Models.TesException;
using TesFileType = Tes.Models.TesFileType;
using TesInput = Tes.Models.TesInput;
using TesOutput = Tes.Models.TesOutput;
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
        internal const string PoolHostName = "CoA-TES-HostName";
        internal const string PoolIsDedicated = "CoA-TES-IsDedicated";

        [GeneratedRegex("[^\\?.]*(\\?.*)")]
        private static partial Regex GetQueryStringRegex();

        private const string AzureSupportUrl = "https://portal.azure.com/#blade/Microsoft_Azure_Support/HelpAndSupportBlade/newsupportrequest";
        private const int PoolKeyLength = 55; // 64 max pool name length - 9 chars generating unique pool names
        private const int DefaultCoreCount = 1;
        private const int DefaultMemoryGb = 2;
        private const int DefaultDiskGb = 10;
        private const int defaultBlobxferOneShotBytes = 4_194_304;
        private const int defaultBlobxferChunkSizeBytes = 16_777_216; // max file size = 16 MiB * 50k blocks = 838,860,800,000 bytes
        private const string CromwellPathPrefix = "/cromwell-executions";
        private const string ExecutionsPathPrefix = "/executions";
        private const string CromwellScriptFileName = "script";
        private const string BatchExecutionDirectoryName = "__batch";
        private const string BatchScriptFileName = "batch_script";
        private const string UploadFilesScriptFileName = "upload_files_script";
        private const string DownloadFilesScriptFileName = "download_files_script";
        private const string StartTaskScriptFilename = "start-task.sh";
        private static readonly Regex queryStringRegex = GetQueryStringRegex();
        private readonly string dockerInDockerImageName;
        private readonly string blobxferImageName;
        private readonly string cromwellDrsLocalizerImageName;
        private readonly ILogger logger;
        private readonly IAzureProxy azureProxy;
        private readonly IStorageAccessProvider storageAccessProvider;
        private readonly IEnumerable<string> allowedVmSizes;
        private readonly IBatchQuotaVerifier quotaVerifier;
        private readonly IBatchSkuInformationProvider skuInformationProvider;
        private readonly List<TesTaskStateTransition> tesTaskStateTransitions;
        private readonly bool usePreemptibleVmsOnly;
        private readonly string batchNodesSubnetId;
        private readonly bool disableBatchNodesPublicIpAddress;
        private readonly bool enableBatchAutopool;
        private readonly BatchNodeInfo gen2BatchNodeInfo;
        private readonly BatchNodeInfo gen1BatchNodeInfo;
        private readonly string marthaUrl;
        private readonly string marthaKeyVaultName;
        private readonly string marthaSecretName;
        private readonly string defaultStorageAccountName;
        private readonly string globalStartTaskPath;
        private readonly string globalManagedIdentity;
        private readonly ContainerRegistryProvider containerRegistryProvider;
        private readonly string batchPrefix;
        private readonly IBatchPoolFactory _batchPoolFactory;
        private readonly string taskRunScriptContent;

        private HashSet<string> onlyLogBatchTaskStateOnce = new();

        /// <summary>
        /// Orchestrates <see cref="Tes.Models.TesTask"/>s on Azure Batch
        /// </summary>
        /// <param name="logger">Logger <see cref="ILogger"/></param>
        /// <param name="batchGen1Options">Configuration of <see cref="Options.BatchImageGeneration1Options"/></param>
        /// <param name="batchGen2Options">Configuration of <see cref="Options.BatchImageGeneration2Options"/></param>
        /// <param name="marthaOptions">Configuration of <see cref="Options.MarthaOptions"/></param>
        /// <param name="storageOptions">Configuration of <see cref="Options.StorageOptions"/></param>
        /// <param name="batchImageNameOptions">Configuration of <see cref="Options.BatchImageNameOptions"/></param>
        /// <param name="batchNodesOptions">Configuration of <see cref="Options.BatchNodesOptions"/></param>
        /// <param name="batchSchedulingOptions">Configuration of <see cref="Options.BatchSchedulingOptions"/></param>
        /// <param name="configuration">Configuration <see cref="IConfiguration"/></param>
        /// <param name="azureProxy">Azure proxy <see cref="IAzureProxy"/></param>
        /// <param name="storageAccessProvider">Storage access provider <see cref="IStorageAccessProvider"/></param>
        /// <param name="quotaVerifier">Quota verifier <see cref="IBatchQuotaVerifier"/>></param>
        /// <param name="skuInformationProvider">Sku information provider <see cref="IBatchSkuInformationProvider"/></param>
        /// <param name="containerRegistryProvider">Container registry information <see cref="ContainerRegistryProvider"/></param>
        /// <param name="poolFactory">Batch pool factory <see cref="IBatchPoolFactory"/></param>
        public BatchScheduler(
            ILogger<BatchScheduler> logger,
            IOptions<Options.BatchImageGeneration1Options> batchGen1Options,
            IOptions<Options.BatchImageGeneration2Options> batchGen2Options,
            IOptions<Options.MarthaOptions> marthaOptions,
            IOptions<Options.StorageOptions> storageOptions,
            IOptions<Options.BatchImageNameOptions> batchImageNameOptions,
            IOptions<Options.BatchNodesOptions> batchNodesOptions,
            IOptions<Options.BatchSchedulingOptions> batchSchedulingOptions,
            IConfiguration configuration,
            IAzureProxy azureProxy,
            IStorageAccessProvider storageAccessProvider,
            IBatchQuotaVerifier quotaVerifier,
            IBatchSkuInformationProvider skuInformationProvider,
            ContainerRegistryProvider containerRegistryProvider,
            IBatchPoolFactory poolFactory)
        {
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(configuration);
            ArgumentNullException.ThrowIfNull(azureProxy);
            ArgumentNullException.ThrowIfNull(storageAccessProvider);
            ArgumentNullException.ThrowIfNull(quotaVerifier);
            ArgumentNullException.ThrowIfNull(skuInformationProvider);
            ArgumentNullException.ThrowIfNull(containerRegistryProvider);
            ArgumentNullException.ThrowIfNull(poolFactory);

            this.logger = logger;
            this.azureProxy = azureProxy;
            this.storageAccessProvider = storageAccessProvider;
            this.quotaVerifier = quotaVerifier;
            this.skuInformationProvider = skuInformationProvider;
            this.containerRegistryProvider = containerRegistryProvider;

            this.allowedVmSizes = GetStringValue(configuration, "AllowedVmSizes", null)?.Split(',', StringSplitOptions.RemoveEmptyEntries).ToList();
            this.usePreemptibleVmsOnly = batchSchedulingOptions.Value.UsePreemptibleVmsOnly;
            this.batchNodesSubnetId = batchNodesOptions.Value.SubnetId;
            this.dockerInDockerImageName = batchImageNameOptions.Value.Docker;
            if (string.IsNullOrWhiteSpace(this.dockerInDockerImageName)) { this.dockerInDockerImageName = Options.BatchImageNameOptions.DefaultDocker; }
            this.blobxferImageName = batchImageNameOptions.Value.Blobxfer;
            if (string.IsNullOrWhiteSpace(this.blobxferImageName)) { this.blobxferImageName = Options.BatchImageNameOptions.DefaultBlobxfer; }
            this.cromwellDrsLocalizerImageName = marthaOptions.Value.CromwellDrsLocalizer;
            if (string.IsNullOrWhiteSpace(this.cromwellDrsLocalizerImageName)) { this.cromwellDrsLocalizerImageName = Options.MarthaOptions.DefaultCromwellDrsLocalizer; }
            this.disableBatchNodesPublicIpAddress = batchNodesOptions.Value.DisablePublicIpAddress;
            this.enableBatchAutopool = batchSchedulingOptions.Value.UseLegacyAutopools;
            this.defaultStorageAccountName = storageOptions.Value.DefaultAccountName;
            this.marthaUrl = marthaOptions.Value.Url;
            this.marthaKeyVaultName = marthaOptions.Value.KeyVaultName;
            this.marthaSecretName = marthaOptions.Value.SecretName;
            this.globalStartTaskPath = StandardizeStartTaskPath(batchNodesOptions.Value.GlobalStartTask, this.defaultStorageAccountName);
            this.globalManagedIdentity = batchNodesOptions.Value.GlobalManagedIdentity;

            if (!this.enableBatchAutopool)
            {
                _batchPoolFactory = poolFactory;
                batchPrefix = batchSchedulingOptions.Value.Prefix;
                logger.LogInformation("BatchPrefix: {BatchPrefix}", batchPrefix);
                taskRunScriptContent = string.Join(") && (", File.ReadAllLines(Path.Combine(AppContext.BaseDirectory, "scripts/task-run.sh")));
            }

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

            logger.LogInformation($"usePreemptibleVmsOnly: {usePreemptibleVmsOnly}");

            static string GetStringValue(IConfiguration configuration, string key, string defaultValue = "") => string.IsNullOrWhiteSpace(configuration[key]) ? defaultValue : configuration[key];

            static bool tesTaskIsQueuedInitializingOrRunning(TesTask tesTask) => tesTask.State == TesState.QUEUEDEnum || tesTask.State == TesState.INITIALIZINGEnum || tesTask.State == TesState.RUNNINGEnum;
            static bool tesTaskIsInitializingOrRunning(TesTask tesTask) => tesTask.State == TesState.INITIALIZINGEnum || tesTask.State == TesState.RUNNINGEnum;
            static bool tesTaskIsQueuedOrInitializing(TesTask tesTask) => tesTask.State == TesState.QUEUEDEnum || tesTask.State == TesState.INITIALIZINGEnum;
            static bool tesTaskIsQueued(TesTask tesTask) => tesTask.State == TesState.QUEUEDEnum;
            static bool tesTaskCancellationRequested(TesTask tesTask) => tesTask.State == TesState.CANCELEDEnum && tesTask.IsCancelRequested;

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
                    tesTask.AddToSystemLog(new [] { batchInfo.AlternateSystemLogItem });
                }
            }

            async Task SetTaskCompleted(TesTask tesTask, CombinedBatchTaskInfo batchInfo)
            {
                await DeleteBatchJobAndPoolIfExists(azureProxy, tesTask, batchInfo);
                SetTaskStateAndLog(tesTask, TesState.COMPLETEEnum, batchInfo);
            }

            async Task SetTaskExecutorError(TesTask tesTask, CombinedBatchTaskInfo batchInfo)
            {
                await DeleteBatchJobAndPoolIfExists(azureProxy, tesTask, batchInfo);
                SetTaskStateAndLog(tesTask, TesState.EXECUTORERROREnum, batchInfo);
            }

            async Task SetTaskSystemError(TesTask tesTask, CombinedBatchTaskInfo batchInfo)
            {
                await DeleteBatchJobAndPoolIfExists(azureProxy, tesTask, batchInfo);
                SetTaskStateAndLog(tesTask, TesState.SYSTEMERROREnum, batchInfo);
            }

            async Task DeleteBatchJobAndSetTaskStateAsync(TesTask tesTask, TesState newTaskState, CombinedBatchTaskInfo batchInfo)
            {
                await DeleteBatchJobOrTaskAsync(tesTask.Id, batchInfo.Pool);
                await azureProxy.DeleteBatchPoolIfExistsAsync(tesTask.Id);
                SetTaskStateAndLog(tesTask, newTaskState, batchInfo);
            }

            Task DeleteBatchJobAndSetTaskExecutorErrorAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo) => DeleteBatchJobAndSetTaskStateAsync(tesTask, TesState.EXECUTORERROREnum, batchInfo);
            Task DeleteBatchJobAndSetTaskSystemErrorAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo) => DeleteBatchJobAndSetTaskStateAsync(tesTask, TesState.SYSTEMERROREnum, batchInfo);

            Task DeleteBatchJobAndRequeueTaskAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo)
                => ++tesTask.ErrorCount > 3
                    ? AddSystemLogAndDeleteBatchJobAndSetTaskExecutorErrorAsync(tesTask, batchInfo, "System Error: Retry count exceeded.")
                    : DeleteBatchJobAndSetTaskStateAsync(tesTask, TesState.QUEUEDEnum, batchInfo);

            Task AddSystemLogAndDeleteBatchJobAndSetTaskExecutorErrorAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo, string alternateSystemLogItem)
            {
                batchInfo.SystemLogItems ??= Enumerable.Empty<string>().Append(alternateSystemLogItem);
                return DeleteBatchJobAndSetTaskExecutorErrorAsync(tesTask, batchInfo);
            }

            async Task CancelTaskAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo)
            {
                await DeleteBatchJobOrTaskAsync(tesTask.Id, batchInfo.Pool);
                await azureProxy.DeleteBatchPoolIfExistsAsync(tesTask.Id);
                tesTask.IsCancelRequested = false;
            }

            Task HandlePreemptedNodeAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo)
            {
                if (enableBatchAutopool)
                {
                    return DeleteBatchJobAndRequeueTaskAsync(tesTask, batchInfo);
                }
                else
                {
                    logger.LogInformation("The TesTask {TesTask}'s node was preempted. It will be automatically rescheduled.", tesTask.Id);
                    return Task.FromResult(false);
                }
            }

            tesTaskStateTransitions = new List<TesTaskStateTransition>()
            {
                new TesTaskStateTransition(tesTaskCancellationRequested, batchTaskState: null, alternateSystemLogItem: null, CancelTaskAsync),
                new TesTaskStateTransition(tesTaskIsQueued, BatchTaskState.JobNotFound, alternateSystemLogItem: null, (tesTask, _) => AddBatchTaskAsync(tesTask)),
                new TesTaskStateTransition(tesTaskIsQueued, BatchTaskState.MissingBatchTask, alternateSystemLogItem: null, (tesTask, batchInfo) => enableBatchAutopool ? DeleteBatchJobAndRequeueTaskAsync(tesTask, batchInfo) : AddBatchTaskAsync(tesTask)),
                new TesTaskStateTransition(tesTaskIsQueued, BatchTaskState.Initializing, alternateSystemLogItem: null, (tesTask, _) => tesTask.State = TesState.INITIALIZINGEnum),
                new TesTaskStateTransition(tesTaskIsQueuedOrInitializing, BatchTaskState.NodeAllocationFailed, alternateSystemLogItem: null, DeleteBatchJobAndRequeueTaskAsync),
                new TesTaskStateTransition(tesTaskIsQueuedOrInitializing, BatchTaskState.Running, alternateSystemLogItem: null, (tesTask, _) => tesTask.State = TesState.RUNNINGEnum),
                new TesTaskStateTransition(tesTaskIsQueuedInitializingOrRunning, BatchTaskState.MoreThanOneActiveJobOrTaskFound, BatchTaskState.MoreThanOneActiveJobOrTaskFound.ToString(), DeleteBatchJobAndSetTaskSystemErrorAsync),
                new TesTaskStateTransition(tesTaskIsQueuedInitializingOrRunning, BatchTaskState.CompletedSuccessfully, alternateSystemLogItem: null, SetTaskCompleted),
                new TesTaskStateTransition(tesTaskIsQueuedInitializingOrRunning, BatchTaskState.CompletedWithErrors, "Please open an issue. There should have been an error reported here.", SetTaskExecutorError),
                new TesTaskStateTransition(tesTaskIsQueuedInitializingOrRunning, BatchTaskState.ActiveJobWithMissingAutoPool, alternateSystemLogItem: null, DeleteBatchJobAndRequeueTaskAsync),
                new TesTaskStateTransition(tesTaskIsQueuedInitializingOrRunning, BatchTaskState.NodeFailedDuringStartupOrExecution, "Please open an issue. There should have been an error reported here.", DeleteBatchJobAndSetTaskExecutorErrorAsync),
                new TesTaskStateTransition(tesTaskIsQueuedInitializingOrRunning, BatchTaskState.NodeUnusable, "Please open an issue. There should have been an error reported here.", DeleteBatchJobAndSetTaskExecutorErrorAsync),
                new TesTaskStateTransition(tesTaskIsInitializingOrRunning, BatchTaskState.JobNotFound, BatchTaskState.JobNotFound.ToString(), SetTaskSystemError),
                new TesTaskStateTransition(tesTaskIsInitializingOrRunning, BatchTaskState.MissingBatchTask, BatchTaskState.MissingBatchTask.ToString(), DeleteBatchJobAndSetTaskSystemErrorAsync),
                new TesTaskStateTransition(tesTaskIsInitializingOrRunning, BatchTaskState.NodePreempted, alternateSystemLogItem: null, HandlePreemptedNodeAsync)
            };
        }

        private Task DeleteBatchJobOrTaskAsync(string taskId, PoolInformation poolInformation, CancellationToken cancellationToken = default)
            => enableBatchAutopool ? azureProxy.DeleteBatchJobAsync(taskId, cancellationToken) : poolInformation is null || poolInformation.PoolId is null ? Task.CompletedTask : azureProxy.DeleteBatchTaskAsync(taskId, poolInformation, cancellationToken);

        private async Task DeleteBatchJobAndPoolIfExists(IAzureProxy azureProxy, TesTask tesTask, CombinedBatchTaskInfo batchInfo)
        {
            var batchDeletionExceptions = new List<Exception>();

            try
            {
                await DeleteBatchJobOrTaskAsync(tesTask.Id, batchInfo.Pool);
            }
            catch (Exception exc)
            {
                logger.LogError(exc, $"Exception deleting batch job with tesTask.Id: {tesTask?.Id}");
                batchDeletionExceptions.Add(exc);
            }

            if (this.enableBatchAutopool)
            {
                try
                {
                    await azureProxy.DeleteBatchPoolIfExistsAsync(tesTask.Id);
                }
                catch (Exception exc)
                {
                    logger.LogError(exc, $"Exception deleting batch pool with tesTask.Id: {tesTask?.Id}");
                    batchDeletionExceptions.Add(exc);
                }
            }

            if (batchDeletionExceptions.Any())
            {
                throw new AggregateException(batchDeletionExceptions);
            }
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudPool> GetCloudPools()
            => azureProxy.GetActivePoolsAsync(this.batchPrefix);

        /// <inheritdoc/>
        public async Task LoadExistingPoolsAsync()
        {
            if (!enableBatchAutopool)
            {
                await foreach (var cloudPool in GetCloudPools())
                {
                    try
                    {
                        var batchPool = _batchPoolFactory.CreateNew();
                        await batchPool.AssignPoolAsync(cloudPool, CancellationToken.None);
                    }
                    catch (Exception exc)
                    {
                        logger.LogError(exc, "When retrieving previously created batch pools and jobs, there were one or more failures when trying to use batch pool {PoolId} or its associated job.", cloudPool.Id);
                    }
                }
            }
        }

        /// <summary>
        /// Iteratively manages execution of a <see cref="TesTask"/> on Azure Batch until completion or failure
        /// </summary>
        /// <param name="tesTask">The <see cref="TesTask"/></param>
        /// <returns>True if the TES task needs to be persisted.</returns>
        public async ValueTask<bool> ProcessTesTaskAsync(TesTask tesTask)
        {
            var combinedBatchTaskInfo = await GetBatchTaskStateAsync(tesTask);
            var msg = $"TES task: {tesTask.Id} TES task state: {tesTask.State} BatchTaskState: {combinedBatchTaskInfo.BatchTaskState}";

            if (!onlyLogBatchTaskStateOnce.Contains(msg))
            {
                logger.LogInformation(msg);
                onlyLogBatchTaskStateOnce.Add(msg);
            }

            return await HandleTesTaskTransitionAsync(tesTask, combinedBatchTaskInfo);
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

        private static string GetCromwellExecutionDirectoryPath(TesTask task)
            => GetParentPath(task.Inputs?.FirstOrDefault(IsCromwellCommandScript)?.Path.TrimStart('/'));

        private static string GetBatchExecutionDirectoryPath(TesTask task)
        {
            var cromwellDir = GetCromwellExecutionDirectoryPath(task);
            if (cromwellDir is not null)
            {
                return $"{cromwellDir}/{BatchExecutionDirectoryName}";
            }
            else
            {
                return $"{ExecutionsPathPrefix.TrimStart('/')}";
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
        /// Determines if the <see cref="Tes.Models.TesInput"/> file is a Cromwell command script
        /// </summary>
        /// <param name="inputFile"><see cref="Tes.Models.TesInput"/> file</param>
        /// <returns>True if the file is a Cromwell command script</returns>
        private static bool IsCromwellCommandScript(TesInput inputFile)
            => inputFile.Name.Equals("commandScript");

        /// <summary>
        /// Verifies existence and translates local file URLs to absolute paths (e.g. file:///tmp/cwl_temp_dir_8026387118450035757/args.py becomes /tmp/cwl_temp_dir_8026387118450035757/args.py)
        /// Only considering files in /cromwell-tmp because that is the only local directory mapped from Cromwell container
        /// </summary>
        /// <param name="fileUri">File URI</param>
        /// <param name="localPath">Local path</param>
        /// <returns></returns>
        private bool TryGetCromwellTmpFilePath(string fileUri, out string localPath)
        {
            localPath = Uri.TryCreate(fileUri, UriKind.Absolute, out var uri) && uri.IsFile && uri.AbsolutePath.StartsWith("/cromwell-tmp/") && this.azureProxy.LocalFileExists(uri.AbsolutePath) ? uri.AbsolutePath : null;

            return localPath is not null;
        }

        /// <summary>
        /// Adds a new Azure Batch pool/job/task for the given <see cref="TesTask"/>
        /// </summary>
        /// <param name="tesTask">The <see cref="TesTask"/> to schedule on Azure Batch</param>
        /// <returns>A task to await</returns>
        private async Task AddBatchTaskAsync(TesTask tesTask)
        {
            PoolInformation poolInformation = null;
            string poolKey = null;

            try
            {
                var virtualMachineInfo = await GetVmSizeAsync(tesTask);

                (poolKey, var displayName) = this.enableBatchAutopool ? default : await GetPoolKey(tesTask, virtualMachineInfo);
                await quotaVerifier.CheckBatchAccountQuotasAsync(virtualMachineInfo, this.enableBatchAutopool || !IsPoolAvailable(poolKey));

                var tesTaskLog = tesTask.AddTesTaskLog();
                tesTaskLog.VirtualMachineInfo = virtualMachineInfo;

                // TODO?: Support for multiple executors. Cromwell has single executor per task.
                var containerConfiguration = await GetContainerConfigurationIfNeededAsync(tesTask.Executors.First().Image);
                var identities = new List<string>();

                if (!string.IsNullOrWhiteSpace(globalManagedIdentity))
                {
                    identities.Add(globalManagedIdentity);
                }

                if (tesTask.Resources?.ContainsBackendParameterValue(TesResources.SupportedBackendParameters.workflow_execution_identity) == true)
                {
                    identities.Add(tesTask.Resources?.GetBackendParameterValue(TesResources.SupportedBackendParameters.workflow_execution_identity));
                }

                var useGen2 = virtualMachineInfo.HyperVGenerations?.Contains("V2");
                string jobOrTaskId = default;
                if (this.enableBatchAutopool)
                {
                    jobOrTaskId = await azureProxy.GetNextBatchJobIdAsync(tesTask.Id);
                    poolInformation = await CreateAutoPoolModePoolInformation(
                        poolSpecification: await GetPoolSpecification(
                        vmSize: virtualMachineInfo.VmSize,
                        autoscaled: false,
                        preemptable: virtualMachineInfo.LowPriority,
                        nodeInfo: useGen2.GetValueOrDefault() ? gen2BatchNodeInfo : gen1BatchNodeInfo,
                        containerConfiguration: containerConfiguration,
                        encryptionAtHostSupported: virtualMachineInfo.EncryptionAtHostSupported),
                    tesTaskId: tesTask.Id,
                    jobId: jobOrTaskId,
                    identityResourceIds: identities);
                }
                else
                {
                    poolInformation = (await GetOrAddPoolAsync(
                        key: poolKey,
                        isPreemptable: virtualMachineInfo.LowPriority,
                        modelPoolFactory: async id => ConvertPoolSpecificationToModelsPool(
                            name: id,
                            displayName: displayName,
                            poolIdentity: GetBatchPoolIdentity(identities.ToArray()),
                            pool: await GetPoolSpecification(
                                vmSize: virtualMachineInfo.VmSize,
                                autoscaled: true,
                                preemptable: virtualMachineInfo.LowPriority,
                                nodeInfo: useGen2.GetValueOrDefault() ? gen2BatchNodeInfo : gen1BatchNodeInfo,
                                containerConfiguration: containerConfiguration,
                                encryptionAtHostSupported: virtualMachineInfo.EncryptionAtHostSupported)))
                        ).Pool;
                    jobOrTaskId = $"{tesTask.Id}-{tesTask.Logs.Count}";
                }

                tesTask.PoolId = poolInformation.PoolId;
                var cloudTask = await ConvertTesTaskToBatchTaskAsync(enableBatchAutopool ? tesTask.Id : jobOrTaskId, tesTask, containerConfiguration is not null, virtualMachineInfo?.VCpusAvailable ?? 1);
                logger.LogInformation($"Creating batch task for TES task {tesTask.Id}. Using VM size {virtualMachineInfo.VmSize}.");

                if (this.enableBatchAutopool)
                {
                    await azureProxy.CreateAutoPoolModeBatchJobAsync(jobOrTaskId, cloudTask, poolInformation);
                }
                else
                {
                    await azureProxy.AddBatchTaskAsync(tesTask.Id, cloudTask, poolInformation);
                }

                tesTaskLog.StartTime = DateTimeOffset.UtcNow;
                tesTask.State = TesState.INITIALIZINGEnum;
                poolInformation = null;
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
            finally
            {
                if (enableBatchAutopool && poolInformation is not null && poolInformation.AutoPoolSpecification is null)
                {
                    await azureProxy.DeleteBatchPoolIfExistsAsync(poolInformation.PoolId);
                }
            }

            void HandleException(Exception exception)
            {
                switch (exception)
                {
                    case AzureBatchPoolCreationException azureBatchPoolCreationException:
                        logger.LogWarning(azureBatchPoolCreationException, "TES task: {TesTask} AzureBatchPoolCreationException.Message: {ExceptionMessage} . This might be a transient issue. Task will remain with state QUEUED.", tesTask.Id, azureBatchPoolCreationException.Message);
                        break;

                    case AzureBatchQuotaMaxedOutException azureBatchQuotaMaxedOutException:
                        logger.LogWarning("TES task: {TesTask} AzureBatchQuotaMaxedOutException.Message: {ExceptionMessage} . Not enough quota available. Task will remain with state QUEUED.", tesTask.Id, azureBatchQuotaMaxedOutException.Message);
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

                    case BatchException batchException when batchException.InnerException is Microsoft.Azure.Batch.Protocol.Models.BatchErrorException batchErrorException && IsJobQuotaException(batchErrorException.Body.Code):
                        tesTask.SetWarning(batchErrorException.Body.Message.Value, Array.Empty<string>());
                        logger.LogInformation("Not enough job quota available for task Id {TesTask}. Reason: {BodyMessage}. Task will remain in queue.", tesTask.Id, batchErrorException.Body.Message.Value);
                        break;

                    case BatchException batchException when batchException.InnerException is Microsoft.Azure.Batch.Protocol.Models.BatchErrorException batchErrorException && IsPoolQuotaException(batchErrorException.Body.Code):
                        neededPools.Add(poolKey);
                        tesTask.SetWarning(batchErrorException.Body.Message.Value, Array.Empty<string>());
                        logger.LogInformation("Not enough pool quota available for task Id {TesTask}. Reason: {BodyMessage}. Task will remain in queue.", tesTask.Id, batchErrorException.Body.Message.Value);
                        break;

                    case Microsoft.Rest.Azure.CloudException cloudException when IsPoolQuotaException(cloudException.Body.Code):
                        neededPools.Add(poolKey);
                        tesTask.SetWarning(cloudException.Body.Message, Array.Empty<string>());
                        logger.LogInformation("Not enough pool quota available for task Id {TesTask}. Reason: {BodyMessage}. Task will remain in queue.", tesTask.Id, cloudException.Body.Message);
                        break;

                    default:
                        tesTask.State = TesState.SYSTEMERROREnum;
                        tesTask.SetFailureReason("UnknownError", exception.Message, exception.StackTrace);
                        logger.LogError(exception, "TES task: {TesTask} Exception.Message: {ExceptionMessage}", tesTask.Id, exception.Message);
                        break;
                }

                static bool IsJobQuotaException(string code)
                    => @"ActiveJobAndScheduleQuotaReached".Equals(code, StringComparison.OrdinalIgnoreCase);

                static bool IsPoolQuotaException(string code)
                    => code switch
                    {
                        "AutoPoolCreationFailedWithQuotaReached" => true,
                        "PoolQuotaReached" => true,
                        _ => false,
                    };
            }
        }

        /// <summary>
        /// Gets the current state of the Azure Batch task
        /// </summary>
        /// <param name="tesTask"><see cref="TesTask"/></param>
        /// <returns>A higher-level abstraction of the current state of the Azure Batch task</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1826:Do not use Enumerable methods on indexable collections", Justification = "FirstOrDefault() is straightforward, the alternative is less clear.")]
        private async ValueTask<CombinedBatchTaskInfo> GetBatchTaskStateAsync(TesTask tesTask)
        {
            var azureBatchJobAndTaskState = await azureProxy.GetBatchJobAndTaskStateAsync(tesTask, enableBatchAutopool);

            if (enableBatchAutopool)
            {
                tesTask.PoolId ??= azureBatchJobAndTaskState.Pool?.PoolId;
            }

            if (azureBatchJobAndTaskState.Pool?.PoolId is null)
            {
                azureBatchJobAndTaskState.Pool = tesTask.PoolId is null ? default : new() { PoolId = tesTask.PoolId };
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
                    Pool = azureBatchJobAndTaskState.Pool
                };
            }

            if (azureBatchJobAndTaskState.MoreThanOneActiveJobOrTaskFound)
            {
                return new CombinedBatchTaskInfo
                {
                    BatchTaskState = BatchTaskState.MoreThanOneActiveJobOrTaskFound,
                    FailureReason = BatchTaskState.MoreThanOneActiveJobOrTaskFound.ToString(),
                    Pool = azureBatchJobAndTaskState.Pool
                };
            }

            // Because a ComputeTask is not assigned to the compute node while the StartTask is running, IAzureProxy.GetBatchJobAndTaskStateAsync() does not see start task failures. Deal with that here.
            if (azureBatchJobAndTaskState.NodeState is null && azureBatchJobAndTaskState.JobState == JobState.Active && azureBatchJobAndTaskState.TaskState == TaskState.Active && !string.IsNullOrWhiteSpace(azureBatchJobAndTaskState.Pool?.PoolId))
            {
                if (this.enableBatchAutopool)
                {
                    _ = ProcessStartTaskFailure((await azureProxy.ListComputeNodesAsync(azureBatchJobAndTaskState.Pool.PoolId, new ODATADetailLevel { FilterClause = "state eq 'starttaskfailed'", SelectClause = "id,startTaskInfo" }).FirstOrDefaultAsync())?.StartTaskInformation?.FailureInformation);
                }
                else
                {
                    /*
                     * Priority order for assigning errors to TesTasks in shared-pool mode:
                     * 1. Node error found in GetBatchJobAndTaskStateAsync()
                     * 2. StartTask failure
                     * 3. NodeAllocation failure
                     */
                    if (TryGetPool(azureBatchJobAndTaskState.Pool.PoolId, out var pool))
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
                }

                bool ProcessStartTaskFailure(TaskFailureInformation failureInformation)
                {
                    if (failureInformation is not null)
                    {
                        azureBatchJobAndTaskState.NodeState = ComputeNodeState.StartTaskFailed;
                        azureBatchJobAndTaskState.NodeErrorCode = failureInformation.Code;
                        azureBatchJobAndTaskState.NodeErrorDetails = failureInformation.Details?.Select(d => d.Value);
                    }

                    return failureInformation is not null;
                }
            }

            switch (azureBatchJobAndTaskState.JobState)
            {
                case null:
                case JobState.Deleting:
                    return new CombinedBatchTaskInfo
                    {
                        BatchTaskState = BatchTaskState.JobNotFound,
                        FailureReason = BatchTaskState.JobNotFound.ToString(),
                        Pool = azureBatchJobAndTaskState.Pool
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
                                Pool = azureBatchJobAndTaskState.Pool
                            };
                        }

                        if (azureBatchJobAndTaskState.NodeState == ComputeNodeState.Unusable)
                        {
                            return new CombinedBatchTaskInfo
                            {
                                BatchTaskState = BatchTaskState.NodeUnusable,
                                FailureReason = BatchTaskState.NodeUnusable.ToString(),
                                SystemLogItems = ConvertNodeErrorsToSystemLogItems(azureBatchJobAndTaskState),
                                Pool = azureBatchJobAndTaskState.Pool
                            };
                        }

                        if (azureBatchJobAndTaskState.NodeState == ComputeNodeState.Preempted)
                        {
                            return new CombinedBatchTaskInfo
                            {
                                BatchTaskState = BatchTaskState.NodePreempted,
                                FailureReason = BatchTaskState.NodePreempted.ToString(),
                                SystemLogItems = ConvertNodeErrorsToSystemLogItems(azureBatchJobAndTaskState),
                                Pool = azureBatchJobAndTaskState.Pool
                            };
                        }

                        if (azureBatchJobAndTaskState.NodeErrorCode is not null)
                        {
                            if (azureBatchJobAndTaskState.NodeErrorCode == TaskFailureInformationCodes.DiskFull)
                            {
                                return new CombinedBatchTaskInfo
                                {
                                    BatchTaskState = BatchTaskState.NodeFailedDuringStartupOrExecution,
                                    FailureReason = azureBatchJobAndTaskState.NodeErrorCode,
                                    SystemLogItems = Enumerable.Empty<string>().Append(azureBatchJobAndTaskState.NodeErrorCode),
                                    Pool = azureBatchJobAndTaskState.Pool
                                };
                            }
                            else
                            {
                                return new CombinedBatchTaskInfo
                                {
                                    BatchTaskState = BatchTaskState.NodeFailedDuringStartupOrExecution,
                                    FailureReason = BatchTaskState.NodeFailedDuringStartupOrExecution.ToString(),
                                    SystemLogItems = ConvertNodeErrorsToSystemLogItems(azureBatchJobAndTaskState),
                                    Pool = azureBatchJobAndTaskState.Pool
                                };
                            }
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
                        Pool = azureBatchJobAndTaskState.Pool
                    };
                case TaskState.Active:
                case TaskState.Preparing:
                    return new CombinedBatchTaskInfo
                    {
                        BatchTaskState = BatchTaskState.Initializing,
                        Pool = azureBatchJobAndTaskState.Pool
                    };
                case TaskState.Running:
                    return new CombinedBatchTaskInfo
                    {
                        BatchTaskState = BatchTaskState.Running,
                        Pool = azureBatchJobAndTaskState.Pool
                    };
                case TaskState.Completed:
                    if (azureBatchJobAndTaskState.TaskExitCode == 0 && azureBatchJobAndTaskState.TaskFailureInformation is null)
                    {
                        var metrics = await GetBatchNodeMetricsAndCromwellResultCodeAsync(tesTask);

                        return new CombinedBatchTaskInfo
                        {
                            BatchTaskState = BatchTaskState.CompletedSuccessfully,
                            BatchTaskExitCode = azureBatchJobAndTaskState.TaskExitCode,
                            BatchTaskStartTime = metrics.TaskStartTime ?? azureBatchJobAndTaskState.TaskStartTime,
                            BatchTaskEndTime = metrics.TaskEndTime ?? azureBatchJobAndTaskState.TaskEndTime,
                            BatchNodeMetrics = metrics.BatchNodeMetrics,
                            CromwellRcCode = metrics.CromwellRcCode,
                            Pool = azureBatchJobAndTaskState.Pool
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
                            Pool = azureBatchJobAndTaskState.Pool
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
        /// <returns>True if the TES task was changed.</returns>
        private async ValueTask<bool> HandleTesTaskTransitionAsync(TesTask tesTask, CombinedBatchTaskInfo combinedBatchTaskInfo)
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

            => await (tesTaskStateTransitions
                .FirstOrDefault(m => (m.Condition is null || m.Condition(tesTask)) && (m.CurrentBatchTaskState is null || m.CurrentBatchTaskState == combinedBatchTaskInfo.BatchTaskState))
                ?.ActionAsync(tesTask, combinedBatchTaskInfo) ?? ValueTask.FromResult(false));

        /// <summary>
        /// Returns job preparation and main Batch tasks that represents the given <see cref="TesTask"/>
        /// </summary>
        /// <param name="taskId">The Batch Task Id</param>
        /// <param name="task">The <see cref="TesTask"/></param>
        /// <param name="poolHasContainerConfig">Indicates that <see cref="CloudTask.ContainerSettings"/> must be set.</param>
        /// <param name="vmVCpusAvailable">The number of VCPUs available in the VM</param>
        /// <returns>Job preparation and main Batch tasks</returns>
        private async Task<CloudTask> ConvertTesTaskToBatchTaskAsync(string taskId, TesTask task, bool poolHasContainerConfig, int vmVCpusAvailable = 1)
        {
            var cromwellExecutionDirectoryPath = GetCromwellExecutionDirectoryPath(task);
            var isCromwell = cromwellExecutionDirectoryPath is not null;
            var batchExecutionPathPrefix = isCromwell ? CromwellPathPrefix : ExecutionsPathPrefix;

            var queryStringsToRemoveFromLocalFilePaths = task.Inputs?
                .Select(i => i.Path)
                .Concat(task.Outputs?.Select(o => o.Path) ?? new List<string>())
                .Where(p => p is not null)
                .Select(p => queryStringRegex.Match(p).Groups[1].Value)
                .Where(qs => !string.IsNullOrEmpty(qs))
                .ToList() ?? new List<string>();

            var inputFiles = task.Inputs?.Distinct().ToList() ?? new List<TesInput>();

            var drsInputFiles = inputFiles
                .Where(f => f?.Url?.StartsWith("drs://", StringComparison.OrdinalIgnoreCase) == true)
                .ToList();

            //foreach (var output in task.Outputs)
            //{
            //    if (!output.Path.StartsWith($"{CromwellPathPrefix}/", StringComparison.OrdinalIgnoreCase) && !output.Path.StartsWith($"{ExecutionsPathPrefix}/", StringComparison.OrdinalIgnoreCase))
            //    {
            //        throw new TesException("InvalidOutputPath", $"Unsupported output path '{output.Path}' for task Id {task.Id}. Must start with {CromwellPathPrefix} or {ExecutionsPathPrefix}");
            //    }
            //}

            var batchExecutionDirectoryPath = GetBatchExecutionDirectoryPath(task);
            var metricsPath = $"/{batchExecutionDirectoryPath}/metrics.txt";
            var metricsUrl = new Uri(await this.storageAccessProvider.MapLocalPathToSasUrlAsync(metricsPath, getContainerSas: true));

            var additionalInputFiles = new List<TesInput>();
            // TODO: Cromwell bug: Cromwell command write_tsv() generates a file in the execution directory, for example execution/write_tsv_3922310b441805fc43d52f293623efbc.tmp. These are not passed on to TES inputs.
            // WORKAROUND: Get the list of files in the execution directory and add them to task inputs.
            if (isCromwell)
            {
                var executionDirectoryUri = new Uri(await this.storageAccessProvider.MapLocalPathToSasUrlAsync($"/{cromwellExecutionDirectoryPath}", getContainerSas: true));
                var blobsInExecutionDirectory = (await azureProxy.ListBlobsAsync(executionDirectoryUri)).Where(b => !b.EndsWith($"/{CromwellScriptFileName}")).Where(b => !b.Contains($"/{BatchExecutionDirectoryName}/"));
                additionalInputFiles = blobsInExecutionDirectory.Select(b => $"{CromwellPathPrefix}/{b}").Select(b => new TesInput { Content = null, Path = b, Url = b, Name = Path.GetFileName(b), Type = TesFileType.FILEEnum }).ToList();
            }

            var filesToDownload = await Task.WhenAll(
                inputFiles
                .Where(f => f?.Url?.StartsWith("drs://", StringComparison.OrdinalIgnoreCase) != true) // do not attempt to download DRS input files since the cromwell-drs-localizer will
                .Union(additionalInputFiles)
                .Select(async f => await GetTesInputFileUrl(f, task.Id, queryStringsToRemoveFromLocalFilePaths)));

            const string exitIfDownloadedFileIsNotFound = "{ [ -f \"$path\" ] && : || { echo \"Failed to download file $url\" 1>&2 && exit 1; } }";
            const string incrementTotalBytesTransferred = "total_bytes=$(( $total_bytes + `stat -c %s \"$path\"` ))";

            int blobxferOneShotBytes = defaultBlobxferOneShotBytes;
            int blobxferChunkSizeBytes = defaultBlobxferChunkSizeBytes;

            if (vmVCpusAvailable >= 4)
            {
                blobxferChunkSizeBytes = 104_857_600; // max file size = 100 MiB * 50k blocks = 5,242,880,000,000 bytes
            }
            else if (vmVCpusAvailable >= 2)
            { 
                blobxferChunkSizeBytes = 33_554_432; // max file size = 32 MiB * 50k blocks = 1,677,721,600,000 bytes
            }

            logger.LogInformation($"Configuring blobxfer with {vmVCpusAvailable} vCPUS to use blobxferChunkSizeBytes={blobxferChunkSizeBytes:D} blobxferOneShotBytes={blobxferOneShotBytes:D}");

            // Using --include and not using --no-recursive as a workaround for https://github.com/Azure/blobxfer/issues/123
            var downloadFilesScriptContent = "total_bytes=0"
                + string.Join("", filesToDownload.Select(f =>
                {
                    var setVariables = $"path='{f.Path}' && url='{f.Url}'";

                    var downloadSingleFile = f.Url.Contains(".blob.core.")
                                                && UrlContainsSas(f.Url) // Workaround for https://github.com/Azure/blobxfer/issues/132
                        ? $"blobxfer download --storage-url \"$url\" --local-path \"$path\" --chunk-size-bytes {blobxferChunkSizeBytes:D} --rename --include '{StorageAccountUrlSegments.Create(f.Url).BlobName}'"
                        : "mkdir -p $(dirname \"$path\") && wget -O \"$path\" \"$url\"";

                    return $" && echo $(date +%T) && {setVariables} && {downloadSingleFile} && {exitIfDownloadedFileIsNotFound} && {incrementTotalBytesTransferred}";
                }))
                + $" && echo FileDownloadSizeInBytes=$total_bytes >> {metricsPath}";

            var downloadFilesScriptPath = $"{batchExecutionDirectoryPath}/{DownloadFilesScriptFileName}";
            var downloadFilesScriptUrl = await this.storageAccessProvider.MapLocalPathToSasUrlAsync($"/{downloadFilesScriptPath}");
            await this.storageAccessProvider.UploadBlobAsync($"/{downloadFilesScriptPath}", downloadFilesScriptContent);
            var filesToUpload = Array.Empty<TesOutput>();

            if (task.Outputs?.Count > 0)
            {
                filesToUpload = await Task.WhenAll(
                    task.Outputs?.Select(async f =>
                        new TesOutput { Path = f.Path, Url = await this.storageAccessProvider.MapLocalPathToSasUrlAsync(f.Url, getContainerSas: true), Name = f.Name, Type = f.Type }));
            }

            // Ignore missing stdout/stderr files. CWL workflows have an issue where if the stdout/stderr are redirected, they are still listed in the TES outputs
            // Ignore any other missing files and directories. WDL tasks can have optional output files.
            // Syntax is: If file or directory doesn't exist, run a noop (":") operator, otherwise run the upload command:
            // { if not exists do nothing else upload; } && { ... }
            var uploadFilesScriptContent = "total_bytes=0"
                + string.Join("", filesToUpload.Select(f =>
                {
                    var setVariables = $"path='{f.Path}' && url='{f.Url}'";
                    var blobxferCommand = $"blobxfer upload --storage-url \"$url\" --local-path \"$path\" --one-shot-bytes {blobxferOneShotBytes:D} --chunk-size-bytes {blobxferChunkSizeBytes:D} {(f.Type == TesFileType.FILEEnum ? "--rename --no-recursive" : string.Empty)}";

                    return $" && {{ {setVariables} && [ ! -e \"$path\" ] && : || {{ {blobxferCommand} && {incrementTotalBytesTransferred}; }} }}";
                }))
                + $" && echo FileUploadSizeInBytes=$total_bytes >> {metricsPath}";

            var uploadFilesScriptPath = $"{batchExecutionDirectoryPath}/{UploadFilesScriptFileName}";
            var uploadFilesScriptSasUrl = await this.storageAccessProvider.MapLocalPathToSasUrlAsync($"/{uploadFilesScriptPath}");
            await this.storageAccessProvider.UploadBlobAsync($"/{uploadFilesScriptPath}", uploadFilesScriptContent);

            var executor = task.Executors.First();

            var volumeMountsOption = $"-v $AZ_BATCH_TASK_WORKING_DIR{batchExecutionPathPrefix}:{batchExecutionPathPrefix}";

            var executorImageIsPublic = containerRegistryProvider.IsImagePublic(executor.Image);
            var dockerInDockerImageIsPublic = containerRegistryProvider.IsImagePublic(dockerInDockerImageName);
            var blobXferImageIsPublic = containerRegistryProvider.IsImagePublic(blobxferImageName);

            var sb = new StringBuilder();

            sb.AppendLinuxLine($"write_kv() {{ echo \"$1=$2\" >> $AZ_BATCH_TASK_WORKING_DIR{metricsPath}; }} && \\");  // Function that appends key=value pair to metrics.txt file
            sb.AppendLinuxLine($"write_ts() {{ write_kv $1 $(date -Iseconds); }} && \\");    // Function that appends key=<current datetime> to metrics.txt file
            sb.AppendLinuxLine($"mkdir -p $AZ_BATCH_TASK_WORKING_DIR/{batchExecutionDirectoryPath} && \\");

            if (dockerInDockerImageIsPublic)
            {
                sb.AppendLinuxLine($"(grep -q alpine /etc/os-release && apk add bash || :) && \\");  // Install bash if running on alpine (will be the case if running inside "docker" image)
            }

            var vmSize = task.Resources?.GetBackendParameterValue(TesResources.SupportedBackendParameters.vm_size);

            if (drsInputFiles.Count > 0 && task.Resources?.ContainsBackendParameterValue(TesResources.SupportedBackendParameters.workflow_execution_identity) == true)
            {
                sb.AppendLinuxLine($"write_ts CromwellDrsLocalizerPullStart && \\");
                sb.AppendLinuxLine($"docker pull --quiet {cromwellDrsLocalizerImageName} && \\");
                sb.AppendLinuxLine($"write_ts CromwellDrsLocalizerPullEnd && \\");
            }

            if (blobXferImageIsPublic)
            {
                sb.AppendLinuxLine($"write_ts BlobXferPullStart && \\");
                sb.AppendLinuxLine($"docker pull --quiet {blobxferImageName} && \\");
                sb.AppendLinuxLine($"write_ts BlobXferPullEnd && \\");
            }

            if (executorImageIsPublic)
            {
                // Private executor images are pulled via pool ContainerConfiguration
                sb.AppendLinuxLine($"write_ts ExecutorPullStart && docker pull --quiet {executor.Image} && write_ts ExecutorPullEnd && \\");
            }

            // The remainder of the script downloads the inputs, runs the main executor container, and uploads the outputs, including the metrics.txt file
            // After task completion, metrics file is downloaded and used to populate the BatchNodeMetrics object
            sb.AppendLinuxLine($"write_kv ExecutorImageSizeInBytes $(docker inspect {executor.Image} | grep \\\"Size\\\" | grep -Po '(?i)\\\"Size\\\":\\K([^,]*)') && \\");

            if (drsInputFiles.Count > 0)
            {
                // resolve DRS input files with Cromwell DRS Localizer Docker image
                sb.AppendLinuxLine($"write_ts DrsLocalizationStart && \\");

                foreach (var drsInputFile in drsInputFiles)
                {
                    var drsUrl = drsInputFile.Url;
                    var localizedFilePath = drsInputFile.Path;
                    var drsLocalizationCommand = $"docker run --rm {volumeMountsOption} -e MARTHA_URL=\"{marthaUrl}\" {cromwellDrsLocalizerImageName} {drsUrl} {localizedFilePath} --access-token-strategy azure{(!string.IsNullOrWhiteSpace(marthaKeyVaultName) ? " --vault-name " + marthaKeyVaultName : string.Empty)}{(!string.IsNullOrWhiteSpace(marthaSecretName) ? " --secret-name " + marthaSecretName : string.Empty)} && \\";
                    sb.AppendLinuxLine(drsLocalizationCommand);
                }

                sb.AppendLinuxLine($"write_ts DrsLocalizationEnd && \\");
            }

            sb.AppendLinuxLine($"write_ts DownloadStart && \\");
            sb.AppendLinuxLine($"docker run --rm {volumeMountsOption} --entrypoint=/bin/sh {blobxferImageName} /{downloadFilesScriptPath} && \\");
            sb.AppendLinuxLine($"write_ts DownloadEnd && \\");
            sb.AppendLinuxLine($"chmod -R o+rwx $AZ_BATCH_TASK_WORKING_DIR{batchExecutionPathPrefix} && \\");
            sb.AppendLinuxLine($"export TES_TASK_WD=$AZ_BATCH_TASK_WORKING_DIR{batchExecutionPathPrefix} && \\");
            sb.AppendLinuxLine($"write_ts ExecutorStart && \\");
            sb.AppendLinuxLine($"docker run --rm {volumeMountsOption} --entrypoint= --workdir / {executor.Image} {executor.Command[0]}  \"{string.Join(" && ", executor.Command.Skip(1))}\" && \\");
            sb.AppendLinuxLine($"write_ts ExecutorEnd && \\");
            sb.AppendLinuxLine($"write_ts UploadStart && \\");
            sb.AppendLinuxLine($"docker run --rm {volumeMountsOption} --entrypoint=/bin/sh {blobxferImageName} /{uploadFilesScriptPath} && \\");
            sb.AppendLinuxLine($"write_ts UploadEnd && \\");
            sb.AppendLinuxLine($"/bin/bash -c 'disk=( `df -k $AZ_BATCH_TASK_WORKING_DIR | tail -1` ) && echo DiskSizeInKiB=${{disk[1]}} >> $AZ_BATCH_TASK_WORKING_DIR{metricsPath} && echo DiskUsedInKiB=${{disk[2]}} >> $AZ_BATCH_TASK_WORKING_DIR{metricsPath}' && \\");
            sb.AppendLinuxLine($"write_kv VmCpuModelName \"$(cat /proc/cpuinfo | grep -m1 name | cut -f 2 -d ':' | xargs)\" && \\");
            sb.AppendLinuxLine($"docker run --rm {volumeMountsOption} {blobxferImageName} upload --storage-url \"{metricsUrl}\" --local-path \"{metricsPath}\" --rename --no-recursive");

            var batchScriptPath = $"{batchExecutionDirectoryPath}/{BatchScriptFileName}";
            await this.storageAccessProvider.UploadBlobAsync($"/{batchScriptPath}", sb.ToString());

            var batchScriptSasUrl = await this.storageAccessProvider.MapLocalPathToSasUrlAsync($"/{batchScriptPath}");
            var batchExecutionDirectorySasUrl = await this.storageAccessProvider.MapLocalPathToSasUrlAsync($"/{batchExecutionDirectoryPath}/", getContainerSas: true);

            var batchRunCommand = enableBatchAutopool
                ? $"/bin/bash {batchScriptPath}"
                : $"/bin/bash -c \"({MungeBatchScriptPath()})\"";

            var cloudTask = new CloudTask(taskId, batchRunCommand)
            {
                UserIdentity = new UserIdentity(new AutoUserSpecification(elevationLevel: ElevationLevel.Admin, scope: AutoUserScope.Pool)),
                ResourceFiles = new List<ResourceFile> { ResourceFile.FromUrl(batchScriptSasUrl, batchScriptPath), ResourceFile.FromUrl(downloadFilesScriptUrl, downloadFilesScriptPath), ResourceFile.FromUrl(uploadFilesScriptSasUrl, uploadFilesScriptPath) },
                OutputFiles = new List<OutputFile> {
                    new OutputFile(
                        "../std*.txt",
                        new OutputFileDestination(new(batchExecutionDirectorySasUrl)),
                        new OutputFileUploadOptions(OutputFileUploadCondition.TaskFailure))
                }
            };

            if (poolHasContainerConfig)
            {
                // If the executor image is private, and in order to run multiple containers in the main task, the image has to be downloaded via pool ContainerConfiguration.
                // This also requires that the main task runs inside a container. So we run the "docker" container that in turn runs other containers.
                // If the executor image is public, there is no need for pool ContainerConfiguration and task can run normally, without being wrapped in a docker container.
                // Volume mapping for docker.sock below allows the docker client in the container to access host's docker daemon.
                var containerRunOptions = $"--rm -v /var/run/docker.sock:/var/run/docker.sock -v $AZ_BATCH_NODE_ROOT_DIR:$AZ_BATCH_NODE_ROOT_DIR ";
                cloudTask.ContainerSettings = new TaskContainerSettings(dockerInDockerImageName, containerRunOptions);
            }

            return cloudTask;

            string MungeBatchScriptPath()
                => (poolHasContainerConfig
                    ? string.Join(") && (", taskRunScriptContent.Split(") && (").SkipWhile(l => l.Contains(@"{TaskExecutor}"))).Replace(">>", ">")
                    : taskRunScriptContent)
                .Replace(@"{BatchScriptPath}", batchScriptPath).Replace(@"{TaskExecutor}", executor.Image).Replace(@"{ExecutionPathPrefix}", batchExecutionPathPrefix.TrimStart('/'));

            static bool UrlContainsSas(string url)
            {
                var uri = new Uri(url, UriKind.Absolute);
                var query = uri.Query;
                return query?.Length > 1 && query[1..].Split('&').Any(QueryContainsSas);

                static bool QueryContainsSas(string arg)
                    => arg switch
                    {
                        var x when x.Split('=', 2)[0] == "sig" => true,
                        var x when x.Contains('=') => false,
                        var x when x.Contains("sas") => true, // PrivatePathsAndUrlsGetSasToken() uses this as a "sas" token
                        _ => false,
                    };
            }
        }

        /// <summary>
        /// Converts the input file URL into proper http URL with SAS token, ready for batch to download.
        /// Removes the query strings from the input file path and the command script content.
        /// Uploads the file if content is provided.
        /// </summary>
        /// <param name="inputFile"><see cref="TesInput"/> file</param>
        /// <param name="taskId">TES task Id</param>
        /// <param name="queryStringsToRemoveFromLocalFilePaths">Query strings to remove from local file paths</param>
        /// <returns>List of modified <see cref="TesInput"/> files</returns>
        private async Task<TesInput> GetTesInputFileUrl(TesInput inputFile, string taskId, List<string> queryStringsToRemoveFromLocalFilePaths)
        {
            if (inputFile.Path is not null
                && !inputFile.Path.StartsWith($"{CromwellPathPrefix}/", StringComparison.OrdinalIgnoreCase)
                && !inputFile.Path.StartsWith($"{ExecutionsPathPrefix}/", StringComparison.OrdinalIgnoreCase))
            {
                throw new TesException("InvalidInputFilePath", $"Unsupported input path '{inputFile.Path}' for task Id {taskId}. Must start with '{CromwellPathPrefix}' or '{ExecutionsPathPrefix}'.");
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

            string inputFileUrl;

            if (inputFile.Content is not null || IsCromwellCommandScript(inputFile))
            {
                inputFileUrl = await this.storageAccessProvider.MapLocalPathToSasUrlAsync(inputFile.Path);

                var content = inputFile.Content ?? await this.storageAccessProvider.DownloadBlobAsync(inputFile.Url);
                content = IsCromwellCommandScript(inputFile) ? RemoveQueryStringsFromLocalFilePaths(content, queryStringsToRemoveFromLocalFilePaths) : content;

                await this.storageAccessProvider.UploadBlobAsync(inputFile.Path, content);
            }
            else if (TryGetCromwellTmpFilePath(inputFile.Url, out var localPath))
            {
                inputFileUrl = await this.storageAccessProvider.MapLocalPathToSasUrlAsync(inputFile.Path);
                await this.storageAccessProvider.UploadBlobFromFileAsync(inputFile.Path, localPath);
            }
            else if (await this.storageAccessProvider.IsPublicHttpUrlAsync(inputFile.Url))
            {
                inputFileUrl = inputFile.Url;
            }
            else
            {
                // Convert file:///account/container/blob paths to /account/container/blob
                var url = Uri.TryCreate(inputFile.Url, UriKind.Absolute, out var tempUrl) && tempUrl.IsFile ? tempUrl.AbsolutePath : inputFile.Url;
                inputFileUrl = (await this.storageAccessProvider.MapLocalPathToSasUrlAsync(url)) ?? throw new TesException("InvalidInputFilePath", $"Unsupported input URL '{inputFile.Url}' for task Id {taskId}. Must start with 'http', '{CromwellPathPrefix}' or use '/accountName/containerName/blobName' pattern where TES service has Contributor access to the storage account.");
            }

            var path = RemoveQueryStringsFromLocalFilePaths(inputFile.Path, queryStringsToRemoveFromLocalFilePaths);
            return new TesInput { Url = inputFileUrl, Path = path };
        }

        /// <summary>
        /// Constructs a universal Azure Start Task instance if needed
        /// </summary>
        /// <returns></returns>
        private async Task<StartTask> StartTaskIfNeeded()
        {
            string startTaskSasUrl = default;

            if (!string.IsNullOrWhiteSpace(globalStartTaskPath))
            {
                startTaskSasUrl = await this.storageAccessProvider.MapLocalPathToSasUrlAsync(globalStartTaskPath);
                if (!await azureProxy.BlobExistsAsync(new Uri(startTaskSasUrl)))
                {
                    startTaskSasUrl = null;
                }
            }

            if (!string.IsNullOrWhiteSpace(startTaskSasUrl) && !string.IsNullOrWhiteSpace(StartTaskScriptFilename))
            {
                return new StartTask
                {
                    CommandLine = $"./{StartTaskScriptFilename}",
                    UserIdentity = new UserIdentity(new AutoUserSpecification(elevationLevel: ElevationLevel.Admin, scope: AutoUserScope.Pool)),
                    ResourceFiles = new List<ResourceFile> { ResourceFile.FromUrl(startTaskSasUrl, StartTaskScriptFilename) }
                };
            }

            return default;
        }

        /// <summary>
        /// Constructs an Azure Batch Container Configuration instance
        /// </summary>
        /// <param name="executorImage">The image name for the current <see cref="TesTask"/></param>
        /// <returns></returns>
        private async ValueTask<ContainerConfiguration> GetContainerConfigurationIfNeededAsync(string executorImage)
        {
            if (containerRegistryProvider.IsImagePublic(executorImage))
            {
                return default;
            }

            BatchModels.ContainerConfiguration result = default;
            var containerRegistryInfo = await containerRegistryProvider.GetContainerRegistryInfoAsync(executorImage);

            if (containerRegistryInfo is not null)
            {
                // Download private images at node startup, since those cannot be downloaded in the main task that runs multiple containers.
                // Doing this also requires that the main task runs inside a container, hence downloading the "docker" image (contains docker client) as well.
                result = new BatchModels.ContainerConfiguration
                {
                    ContainerImageNames = new List<string> { executorImage, dockerInDockerImageName, blobxferImageName },
                    ContainerRegistries = new List<BatchModels.ContainerRegistry>
                    {
                        new(
                            userName: containerRegistryInfo.Username,
                            registryServer: containerRegistryInfo.RegistryServer,
                            password: containerRegistryInfo.Password)
                    }
                };

                ContainerRegistryInfo containerRegistryInfoForDockerInDocker = null;

                if (!containerRegistryProvider.IsImagePublic(dockerInDockerImageName))
                {
                    containerRegistryInfoForDockerInDocker = await containerRegistryProvider.GetContainerRegistryInfoAsync(dockerInDockerImageName);

                    if (containerRegistryInfoForDockerInDocker is not null && containerRegistryInfoForDockerInDocker.RegistryServer != containerRegistryInfo.RegistryServer)
                    {
                        result.ContainerRegistries.Add(new(
                            userName: containerRegistryInfoForDockerInDocker.Username,
                            registryServer: containerRegistryInfoForDockerInDocker.RegistryServer,
                            password: containerRegistryInfoForDockerInDocker.Password));
                    }
                }

                if (!containerRegistryProvider.IsImagePublic(blobxferImageName))
                {
                    var containerRegistryInfoForBlobXfer = await containerRegistryProvider.GetContainerRegistryInfoAsync(blobxferImageName);

                    if (containerRegistryInfoForBlobXfer is not null && containerRegistryInfoForBlobXfer.RegistryServer != containerRegistryInfo.RegistryServer && containerRegistryInfoForBlobXfer.RegistryServer != containerRegistryInfoForDockerInDocker?.RegistryServer)
                    {
                        result.ContainerRegistries.Add(new(
                            userName: containerRegistryInfoForBlobXfer.Username,
                            registryServer: containerRegistryInfoForBlobXfer.RegistryServer,
                            password: containerRegistryInfoForBlobXfer.Password));
                    }
                }
            }

            return result is null ? default : new()
            {
                ContainerImageNames = result.ContainerImageNames,
                ContainerRegistries = result
                                    .ContainerRegistries
                                    .Select(r => new ContainerRegistry(
                                        userName: r.UserName,
                                        password: r.Password,
                                        registryServer: r.RegistryServer,
                                        identityReference: r.IdentityReference is null ? null : new() { ResourceId = r.IdentityReference.ResourceId }))
                                    .ToList()
            };
        }

        /// <summary>
        /// Constructs either an <see cref="AutoPoolSpecification"/> or a new pool in the batch account ready for a job to be attached.
        /// </summary>
        /// <param name="poolSpecification"></param>
        /// <param name="tesTaskId"></param>
        /// <param name="jobId"></param>
        /// <param name="identityResourceIds"></param>
        /// <remarks>If <paramref name="identityResourceIds"/> is provided, <paramref name="jobId"/> must also be provided.<br/>This method does not support autscaled pools.</remarks>
        /// <returns>An <see cref="PoolInformation"/></returns>
        private async Task<PoolInformation> CreateAutoPoolModePoolInformation(PoolSpecification poolSpecification, string tesTaskId, string jobId, IEnumerable<string> identityResourceIds = null)
        {
            var identities = identityResourceIds?.ToArray() ?? Array.Empty<string>();
            var isAutoPool = !identities.Any();

            if (isAutoPool)
            {
                logger.LogInformation($"TES task: {tesTaskId} creating Auto Pool using VM size {poolSpecification.VirtualMachineSize}");
            }
            else
            {
                logger.LogInformation($"TES task: {tesTaskId} creating Manual Batch Pool using VM size {poolSpecification.VirtualMachineSize}");
            }

            // By default, the pool will have the same name/ID as the job if the identity is provided, otherwise we return an actual autopool.
            return isAutoPool
                ? new()
                {
                    AutoPoolSpecification = new()
                    {
                        AutoPoolIdPrefix = "TES",
                        PoolLifetimeOption = PoolLifetimeOption.Job,
                        PoolSpecification = poolSpecification,
                        KeepAlive = false
                    }
                }
                : await azureProxy.CreateBatchPoolAsync(
                    ConvertPoolSpecificationToModelsPool(
                        $"TES_{jobId}",
                        jobId,
                        GetBatchPoolIdentity(identities),
                        poolSpecification),
                    IsPreemptable());

            bool IsPreemptable()
                => true switch
                {
                    _ when poolSpecification.TargetDedicatedComputeNodes > 0 => false,
                    _ when poolSpecification.TargetLowPriorityComputeNodes > 0 => true,
                    _ => throw new ArgumentException("Unable to determine if pool will host a low priority compute node.", nameof(poolSpecification)),
                };
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
        /// <param name="vmSize"></param>
        /// <param name="autoscaled"></param>
        /// <param name="preemptable"></param>
        /// <param name="nodeInfo"></param>
        /// <param name="containerConfiguration"></param>
        /// <returns><see cref="PoolSpecification"/></returns>
        /// <remarks>We use the PoolSpecification for both the namespace of all the constituent parts and for the fact that it allows us to configure shared and autopools using the same code.</remarks>
        private async ValueTask<PoolSpecification> GetPoolSpecification(string vmSize, bool autoscaled, bool preemptable, BatchNodeInfo nodeInfo, ContainerConfiguration containerConfiguration, bool encryptionAtHostSupported)
        {
            // Any changes to any properties set in this method will require corresponding changes to ConvertPoolSpecificationToModelsPool()

            var vmConfig = new VirtualMachineConfiguration(
                imageReference: new ImageReference(
                    nodeInfo.BatchImageOffer,
                    nodeInfo.BatchImagePublisher,
                    nodeInfo.BatchImageSku,
                    nodeInfo.BatchImageVersion),
                nodeAgentSkuId: nodeInfo.BatchNodeAgentSkuId)
            {
                ContainerConfiguration = containerConfiguration
            };

            if (encryptionAtHostSupported)
            {
                vmConfig.DiskEncryptionConfiguration = new DiskEncryptionConfiguration(
                    targets: new List<DiskEncryptionTarget> { DiskEncryptionTarget.OsDisk, DiskEncryptionTarget.TemporaryDisk }
                );
            }

            var poolSpecification = new PoolSpecification
            {
                VirtualMachineConfiguration = vmConfig,
                VirtualMachineSize = vmSize,
                ResizeTimeout = TimeSpan.FromMinutes(30),
                StartTask = await StartTaskIfNeeded(),
                TargetNodeCommunicationMode = NodeCommunicationMode.Simplified,
            };

            if (autoscaled)
            {
                poolSpecification.AutoScaleEnabled = true;
                poolSpecification.AutoScaleEvaluationInterval = BatchPool.AutoScaleEvaluationInterval;
                poolSpecification.AutoScaleFormula = BatchPool.AutoPoolFormula(preemptable, 1);
            }
            else
            {
                poolSpecification.AutoScaleEnabled = false;
                poolSpecification.TargetLowPriorityComputeNodes = preemptable == true ? 1 : 0;
                poolSpecification.TargetDedicatedComputeNodes = preemptable == false ? 1 : 0;
            }

            if (!string.IsNullOrEmpty(this.batchNodesSubnetId))
            {
                poolSpecification.NetworkConfiguration = new()
                {
                    PublicIPAddressConfiguration = new PublicIPAddressConfiguration(this.disableBatchNodesPublicIpAddress ? IPAddressProvisioningType.NoPublicIPAddresses : IPAddressProvisioningType.BatchManaged),
                    SubnetId = this.batchNodesSubnetId
                };
            }

            return poolSpecification;
        }

        /// <summary>
        /// Convert PoolSpecification to Models.Pool, including any BatchPoolIdentity
        /// </summary>
        /// <remarks>
        /// Note: this is not a complete conversion. It only converts properties we are currently using (including referenced objects).<br/>
        /// Devs: Any changes to any properties set in this method will require corresponding changes to all classes implementing <see cref="Management.Batch.IBatchPoolManager"/> along with possibly any systems they call, with the possible exception of <seealso cref="Management.Batch.ArmBatchPoolManager"/>.
        /// </remarks>
        /// <param name="name"></param>
        /// <param name="displayName"></param>
        /// <param name="poolIdentity"></param>
        /// <param name="pool"></param>
        /// <returns>A <see cref="BatchModels.Pool"/>.</returns>
        private static BatchModels.Pool ConvertPoolSpecificationToModelsPool(string name, string displayName, BatchModels.BatchPoolIdentity poolIdentity, PoolSpecification pool)
        {
            // Don't add feature work here that isn't necesitated by a change to GetPoolSpecification() unless it's a feature that PoolSpecification does not support.
            // TODO: (perpetually) add new properties we set in the future on <see cref="PoolSpecification"/> and/or its contained objects, if possible. When not, update CreateAutoPoolModePoolInformation().

            ValidateString(name, nameof(name), 64);
            ValidateString(displayName, nameof(displayName), 1024);

            return new(name: name, displayName: displayName, identity: poolIdentity)
            {
                VmSize = pool.VirtualMachineSize,
                ScaleSettings = true == pool.AutoScaleEnabled ? ConvertAutoScale(pool) : ConvertManualScale(pool),
                DeploymentConfiguration = new(virtualMachineConfiguration: ConvertVirtualMachineConfiguration(pool.VirtualMachineConfiguration)),
                ApplicationPackages = pool.ApplicationPackageReferences?.Select(ConvertApplicationPackage).ToList(),
                NetworkConfiguration = ConvertNetworkConfiguration(pool.NetworkConfiguration),
                StartTask = ConvertStartTask(pool.StartTask),
                TargetNodeCommunicationMode = ConvertNodeCommunicationMode(pool.TargetNodeCommunicationMode),
            };

            static void ValidateString(string value, string name, int length)
            {
                ArgumentNullException.ThrowIfNull(value, name);
                if (value.Length > length) throw new ArgumentException($"{name} exceeds maximum length {length}", name);
            }

            static BatchModels.ScaleSettings ConvertManualScale(PoolSpecification pool)
                => new()
                {
                    FixedScale = new()
                    {
                        TargetDedicatedNodes = pool.TargetDedicatedComputeNodes,
                        TargetLowPriorityNodes = pool.TargetLowPriorityComputeNodes,
                        ResizeTimeout = pool.ResizeTimeout,
                        NodeDeallocationOption = BatchModels.ComputeNodeDeallocationOption.TaskCompletion
                    }
                };

            static BatchModels.ScaleSettings ConvertAutoScale(PoolSpecification pool)
                => new()
                {
                    AutoScale = new()
                    {
                        Formula = pool.AutoScaleFormula,
                        EvaluationInterval = pool.AutoScaleEvaluationInterval
                    }
                };

            static BatchModels.VirtualMachineConfiguration ConvertVirtualMachineConfiguration(VirtualMachineConfiguration virtualMachineConfiguration)
                => virtualMachineConfiguration is null ? default : new(ConvertImageReference(virtualMachineConfiguration.ImageReference), virtualMachineConfiguration.NodeAgentSkuId, containerConfiguration: ConvertContainerConfiguration(virtualMachineConfiguration.ContainerConfiguration), diskEncryptionConfiguration: ConvertDiskEncryptionConfiguration(virtualMachineConfiguration.DiskEncryptionConfiguration));

            static BatchModels.ContainerConfiguration ConvertContainerConfiguration(ContainerConfiguration containerConfiguration)
                => containerConfiguration is null ? default : new(containerConfiguration.ContainerImageNames, containerConfiguration.ContainerRegistries?.Select(ConvertContainerRegistry).ToList());

            static BatchModels.StartTask ConvertStartTask(StartTask startTask)
                => startTask is null ? default : new(startTask.CommandLine, startTask.ResourceFiles?.Select(ConvertResourceFile).ToList(), startTask.EnvironmentSettings?.Select(ConvertEnvironmentSetting).ToList(), ConvertUserIdentity(startTask.UserIdentity), startTask.MaxTaskRetryCount, startTask.WaitForSuccess, ConvertTaskContainerSettings(startTask.ContainerSettings));

            static BatchModels.UserIdentity ConvertUserIdentity(UserIdentity userIdentity)
                => userIdentity is null ? default : new(userIdentity.UserName, ConvertAutoUserSpecification(userIdentity.AutoUser));

            static BatchModels.AutoUserSpecification ConvertAutoUserSpecification(AutoUserSpecification autoUserSpecification)
                => autoUserSpecification is null ? default : new((BatchModels.AutoUserScope?)autoUserSpecification.Scope, (BatchModels.ElevationLevel?)autoUserSpecification.ElevationLevel);

            static BatchModels.TaskContainerSettings ConvertTaskContainerSettings(TaskContainerSettings containerSettings)
                => containerSettings is null ? default : new(containerSettings.ImageName, containerSettings.ContainerRunOptions, ConvertContainerRegistry(containerSettings.Registry), (BatchModels.ContainerWorkingDirectory?)containerSettings.WorkingDirectory);

            static BatchModels.ContainerRegistry ConvertContainerRegistry(ContainerRegistry containerRegistry)
                => containerRegistry is null ? default : new(containerRegistry.UserName, containerRegistry.Password, containerRegistry.RegistryServer, ConvertComputeNodeIdentityReference(containerRegistry.IdentityReference));

            static BatchModels.ResourceFile ConvertResourceFile(ResourceFile resourceFile)
                => resourceFile is null ? default : new(resourceFile.AutoStorageContainerName, resourceFile.StorageContainerUrl, resourceFile.HttpUrl, resourceFile.BlobPrefix, resourceFile.FilePath, resourceFile.FileMode, ConvertComputeNodeIdentityReference(resourceFile.IdentityReference));

            static BatchModels.ComputeNodeIdentityReference ConvertComputeNodeIdentityReference(ComputeNodeIdentityReference computeNodeIdentityReference)
                => computeNodeIdentityReference is null ? default : new(computeNodeIdentityReference.ResourceId);

            static BatchModels.EnvironmentSetting ConvertEnvironmentSetting(EnvironmentSetting environmentSetting)
                => environmentSetting is null ? default : new(environmentSetting.Name, environmentSetting.Value);

            static BatchModels.ImageReference ConvertImageReference(ImageReference imageReference)
                => imageReference is null ? default : new(imageReference.Publisher, imageReference.Offer, imageReference.Sku, imageReference.Version);

            static BatchModels.ApplicationPackageReference ConvertApplicationPackage(ApplicationPackageReference applicationPackage)
                => applicationPackage is null ? default : new(applicationPackage.ApplicationId, applicationPackage.Version);

            static BatchModels.NetworkConfiguration ConvertNetworkConfiguration(NetworkConfiguration networkConfiguration)
                => networkConfiguration is null ? default : new(subnetId: networkConfiguration.SubnetId, publicIPAddressConfiguration: ConvertPublicIPAddressConfiguration(networkConfiguration.PublicIPAddressConfiguration));

            static BatchModels.PublicIPAddressConfiguration ConvertPublicIPAddressConfiguration(PublicIPAddressConfiguration publicIPAddressConfiguration)
                => publicIPAddressConfiguration is null ? default : new(provision: (BatchModels.IPAddressProvisioningType?)publicIPAddressConfiguration.Provision);

            static BatchModels.NodeCommunicationMode? ConvertNodeCommunicationMode(NodeCommunicationMode? nodeCommunicationMode)
                => (BatchModels.NodeCommunicationMode?)nodeCommunicationMode;

            static BatchModels.DiskEncryptionConfiguration? ConvertDiskEncryptionConfiguration(DiskEncryptionConfiguration? diskEncryptionConfiguration)
                => diskEncryptionConfiguration is null ? default : new(diskEncryptionConfiguration.Targets.Select(x => ConvertDiskEncryptionTarget(x)).ToList());

            static BatchModels.DiskEncryptionTarget ConvertDiskEncryptionTarget(DiskEncryptionTarget? diskEncryptionTarget)
                => (BatchModels.DiskEncryptionTarget)diskEncryptionTarget;
        }

        /// <summary>
        /// Removes a set of strings from the given string
        /// </summary>
        /// <param name="stringsToRemove">Strings to remove</param>
        /// <param name="originalString">The original string</param>
        /// <returns>The modified string</returns>
        private static string RemoveQueryStringsFromLocalFilePaths(string originalString, IEnumerable<string> stringsToRemove)
        {
            if (!stringsToRemove.Any(s => originalString.Contains(s, StringComparison.OrdinalIgnoreCase)))
            {
                return originalString;
            }

            var modifiedString = originalString;

            foreach (var stringToRemove in stringsToRemove)
            {
                modifiedString = modifiedString.Replace(stringToRemove, string.Empty, StringComparison.OrdinalIgnoreCase);
            }

            return modifiedString;
        }

        /// <summary>
        /// Gets the cheapest available VM size that satisfies the <see cref="TesTask"/> execution requirements
        /// </summary>
        /// <param name="tesTask"><see cref="TesTask"/></param>
        /// <param name="forcePreemptibleVmsOnly">Force consideration of preemptible virtual machines only.</param>
        /// <returns>The virtual machine info</returns>
        public async Task<VirtualMachineInformation> GetVmSizeAsync(TesTask tesTask, bool forcePreemptibleVmsOnly = false)
        {
            bool allowedVmSizesFilter(VirtualMachineInformation vm) => allowedVmSizes is null || !allowedVmSizes.Any() || allowedVmSizes.Contains(vm.VmSize, StringComparer.OrdinalIgnoreCase) || allowedVmSizes.Contains(vm.VmFamily, StringComparer.OrdinalIgnoreCase);

            var tesResources = tesTask.Resources;

            var previouslyFailedVmSizes = tesTask.Logs?
                .Where(log => log.FailureReason == BatchTaskState.NodeAllocationFailed.ToString() && log.VirtualMachineInfo?.VmSize is not null)
                .Select(log => log.VirtualMachineInfo.VmSize)
                .Distinct()
                .ToList();

            var virtualMachineInfoList = await skuInformationProvider.GetVmSizesAndPricesAsync(azureProxy.GetArmRegion());
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
                .GetVmCoreQuotaAsync(preemptible);

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

                    return await GetVmSizeAsync(tesTask, true);
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

        private async Task<(Tes.Models.BatchNodeMetrics BatchNodeMetrics, DateTimeOffset? TaskStartTime, DateTimeOffset? TaskEndTime, int? CromwellRcCode)> GetBatchNodeMetricsAndCromwellResultCodeAsync(TesTask tesTask)
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
                var cromwellRcContent = await this.storageAccessProvider.DownloadBlobAsync($"/{GetCromwellExecutionDirectoryPath(tesTask)}/rc");

                if (cromwellRcContent is not null && int.TryParse(cromwellRcContent, out var temp))
                {
                    cromwellRcCode = temp;
                }

                var metricsContent = await this.storageAccessProvider.DownloadBlobAsync($"/{GetBatchExecutionDirectoryPath(tesTask)}/metrics.txt");

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

        /// <summary>
        /// Class that captures how <see cref="TesTask"/> transitions from current state to the new state, given the current Batch task state and optional condition. 
        /// Transitions typically include an action that needs to run in order for the task to move to the new state.
        /// </summary>
        private class TesTaskStateTransition
        {
            public TesTaskStateTransition(Func<TesTask, bool> condition, BatchTaskState? batchTaskState, string alternateSystemLogItem, Func<TesTask, CombinedBatchTaskInfo, Task> asyncAction)
                : this(condition, batchTaskState, alternateSystemLogItem, asyncAction, null)
            { }

            public TesTaskStateTransition(Func<TesTask, bool> condition, BatchTaskState? batchTaskState, string alternateSystemLogItem, Action<TesTask, CombinedBatchTaskInfo> action)
                : this(condition, batchTaskState, alternateSystemLogItem, null, action)
            {
            }

            private TesTaskStateTransition(Func<TesTask, bool> condition, BatchTaskState? batchTaskState, string alternateSystemLogItem, Func<TesTask, CombinedBatchTaskInfo, Task> asyncAction, Action<TesTask, CombinedBatchTaskInfo> action)
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
            private Func<TesTask, CombinedBatchTaskInfo, Task> AsyncAction { get; }
            private Action<TesTask, CombinedBatchTaskInfo> Action { get; }

            /// <summary>
            /// Calls <see cref="Action"/> and/or <see cref="AsyncAction"/>.
            /// </summary>
            /// <param name="tesTask"></param>
            /// <param name="combinedBatchTaskInfo"></param>
            /// <returns>True an action was called, otherwise False.</returns>
            public async ValueTask<bool> ActionAsync(TesTask tesTask, CombinedBatchTaskInfo combinedBatchTaskInfo)
            {
                combinedBatchTaskInfo.AlternateSystemLogItem = AlternateSystemLogItem;
                var tesTaskChanged = false;

                if (AsyncAction is not null)
                {
                    await AsyncAction(tesTask, combinedBatchTaskInfo);
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

        private class ExternalStorageContainerInfo
        {
            public string AccountName { get; set; }
            public string ContainerName { get; set; }
            public string BlobEndpoint { get; set; }
            public string SasToken { get; set; }
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
            public PoolInformation Pool { get; set; }
            public string AlternateSystemLogItem { get; set; }
        }
    }
}
