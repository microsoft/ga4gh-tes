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
using Tes.Runner.Models;
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
        private const string CromwellPathPrefix = "/cromwell-executions";
        private const string TesExecutionsPathPrefix = "/tes-internal";
        private const string CromwellScriptFileName = "script";
        private const string BatchScriptFileName = "batch_script";
        private const string UploadFilesScriptFileName = "upload_files_script";
        private const string DownloadFilesScriptFileName = "download_files_script";
        private const string UploadMetricsScriptFileName = "upload_metrics_script";
        private const string StartTaskScriptFilename = "start-task.sh";
        private const string NodeTaskRunnerFilename = "tRunner";
        private const string NodeRunnerTaskInfoFilename = "TesTask.json";
        private const string NodeTaskRunnerMD5HashFilename = "TRunnerMD5Hash.txt";
        private static readonly Regex queryStringRegex = GetQueryStringRegex();
        private readonly string dockerInDockerImageName;
        private readonly string cromwellDrsLocalizerImageName;
        private readonly IConfiguration configuration;
        private readonly ILogger logger;
        private readonly IAzureProxy azureProxy;
        private readonly IStorageAccessProvider storageAccessProvider;
        private readonly IBatchQuotaVerifier quotaVerifier;
        private readonly IBatchSkuInformationProvider skuInformationProvider;
        private readonly List<TesTaskStateTransition> tesTaskStateTransitions;
        private readonly bool usePreemptibleVmsOnly;
        private readonly string batchNodesSubnetId;
        private readonly bool disableBatchNodesPublicIpAddress;
        private readonly bool enableBatchAutopool;
        private readonly TimeSpan poolLifetime;
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
        private readonly string[] taskRunScriptContent;
        private readonly string[] taskCleanupScriptContent;
        private readonly IAllowedVmSizesService allowedVmSizesService;

        private HashSet<string> onlyLogBatchTaskStateOnce = new();

        private static string GetStringValue(IConfiguration configuration, string key, string defaultValue = "") => string.IsNullOrWhiteSpace(configuration[key]) ? defaultValue : configuration[key];
        private IEnumerable<string> allowedVmSizes => GetStringValue(this.configuration, "AllowedVmSizes", null)?.Split(',', StringSplitOptions.RemoveEmptyEntries).ToList();

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
        /// <param name="azureProxy">Azure proxy <see cref="IAzureProxy"/></param>
        /// <param name="storageAccessProvider">Storage access provider <see cref="IStorageAccessProvider"/></param>
        /// <param name="quotaVerifier">Quota verifier <see cref="IBatchQuotaVerifier"/>></param>
        /// <param name="skuInformationProvider">Sku information provider <see cref="IBatchSkuInformationProvider"/></param>
        /// <param name="containerRegistryProvider">Container registry information <see cref="ContainerRegistryProvider"/></param>
        /// <param name="poolFactory">Batch pool factory <see cref="IBatchPoolFactory"/></param>
        /// <param name="allowedVmSizesService">Service to get allowed vm sizes.</param>
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
            IBatchPoolFactory poolFactory,
            IAllowedVmSizesService allowedVmSizesService)
        {
            ArgumentNullException.ThrowIfNull(logger);
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

            this.usePreemptibleVmsOnly = batchSchedulingOptions.Value.UsePreemptibleVmsOnly;
            this.batchNodesSubnetId = batchNodesOptions.Value.SubnetId;
            this.dockerInDockerImageName = batchImageNameOptions.Value.Docker;
            if (string.IsNullOrWhiteSpace(this.dockerInDockerImageName)) { this.dockerInDockerImageName = Options.BatchImageNameOptions.DefaultDocker; }
            this.cromwellDrsLocalizerImageName = marthaOptions.Value.CromwellDrsLocalizer;
            if (string.IsNullOrWhiteSpace(this.cromwellDrsLocalizerImageName)) { this.cromwellDrsLocalizerImageName = Options.MarthaOptions.DefaultCromwellDrsLocalizer; }
            this.disableBatchNodesPublicIpAddress = batchNodesOptions.Value.DisablePublicIpAddress;
            this.enableBatchAutopool = batchSchedulingOptions.Value.UseLegacyAutopools;
            this.poolLifetime = this.enableBatchAutopool ? TimeSpan.Zero : TimeSpan.FromDays(batchSchedulingOptions.Value.PoolRotationForcedDays == 0 ? Options.BatchSchedulingOptions.DefaultPoolRotationForcedDays : batchSchedulingOptions.Value.PoolRotationForcedDays);
            this.defaultStorageAccountName = storageOptions.Value.DefaultAccountName;
            this.marthaUrl = marthaOptions.Value.Url;
            this.marthaKeyVaultName = marthaOptions.Value.KeyVaultName;
            this.marthaSecretName = marthaOptions.Value.SecretName;
            this.globalStartTaskPath = StandardizeStartTaskPath(batchNodesOptions.Value.GlobalStartTask, this.defaultStorageAccountName);
            this.globalManagedIdentity = batchNodesOptions.Value.GlobalManagedIdentity;
            this.allowedVmSizesService = allowedVmSizesService;

            if (!this.enableBatchAutopool)
            {
                _batchPoolFactory = poolFactory;
                batchPrefix = batchSchedulingOptions.Value.Prefix;
                logger.LogInformation("BatchPrefix: {BatchPrefix}", batchPrefix);
                taskRunScriptContent = File.ReadAllLines(Path.Combine(AppContext.BaseDirectory, "scripts/task-run.sh"));
                taskCleanupScriptContent = File.ReadAllLines(Path.Combine(AppContext.BaseDirectory, "scripts/clean-executor.sh"));
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
                    tesTask.AddToSystemLog(new[] { batchInfo.AlternateSystemLogItem });
                }
            }

            async Task SetTaskCompleted(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
            {
                await DeleteBatchTaskAndOrJobAndOrPoolIfExists(azureProxy, tesTask, batchInfo, cancellationToken);
                SetTaskStateAndLog(tesTask, TesState.COMPLETEEnum, batchInfo);
            }

            async Task SetTaskExecutorError(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
            {
                await DeleteBatchTaskAndOrJobAndOrPoolIfExists(azureProxy, tesTask, batchInfo, cancellationToken);
                SetTaskStateAndLog(tesTask, TesState.EXECUTORERROREnum, batchInfo);
            }

            async Task SetTaskSystemError(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
            {
                await DeleteBatchTaskAndOrJobAndOrPoolIfExists(azureProxy, tesTask, batchInfo, cancellationToken);
                SetTaskStateAndLog(tesTask, TesState.SYSTEMERROREnum, batchInfo);
            }

            async Task DeleteBatchJobAndSetTaskStateAsync(TesTask tesTask, TesState newTaskState, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
            {
                await DeleteBatchJobOrTaskAsync(tesTask, batchInfo.Pool, cancellationToken);
                await azureProxy.DeleteBatchPoolIfExistsAsync(tesTask.Id, cancellationToken);
                SetTaskStateAndLog(tesTask, newTaskState, batchInfo);
            }

            Task DeleteBatchJobAndSetTaskExecutorErrorAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken) => DeleteBatchJobAndSetTaskStateAsync(tesTask, TesState.EXECUTORERROREnum, batchInfo, cancellationToken);
            Task DeleteBatchJobAndSetTaskSystemErrorAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken) => DeleteBatchJobAndSetTaskStateAsync(tesTask, TesState.SYSTEMERROREnum, batchInfo, cancellationToken);

            Task DeleteBatchJobAndRequeueTaskAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
                => ++tesTask.ErrorCount > 3
                    ? AddSystemLogAndDeleteBatchJobAndSetTaskExecutorErrorAsync(tesTask, batchInfo, "System Error: Retry count exceeded.", cancellationToken)
                    : DeleteBatchJobAndSetTaskStateAsync(tesTask, TesState.QUEUEDEnum, batchInfo, cancellationToken);

            Task AddSystemLogAndDeleteBatchJobAndSetTaskExecutorErrorAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo, string alternateSystemLogItem, CancellationToken cancellationToken)
            {
                batchInfo.SystemLogItems ??= Enumerable.Empty<string>().Append(alternateSystemLogItem);
                return DeleteBatchJobAndSetTaskExecutorErrorAsync(tesTask, batchInfo, cancellationToken);
            }

            async Task CancelTaskAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
            {
                await DeleteBatchJobOrTaskAsync(tesTask, batchInfo.Pool, cancellationToken);
                await azureProxy.DeleteBatchPoolIfExistsAsync(tesTask.Id, cancellationToken);
                tesTask.IsCancelRequested = false;
            }

            Task HandlePreemptedNodeAsync(TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
            {
                if (enableBatchAutopool)
                {
                    return DeleteBatchJobAndRequeueTaskAsync(tesTask, batchInfo, cancellationToken);
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
                new TesTaskStateTransition(tesTaskIsQueued, BatchTaskState.JobNotFound, alternateSystemLogItem: null, (tesTask, _, ct) => AddBatchTaskAsync(tesTask, ct)),
                new TesTaskStateTransition(tesTaskIsQueued, BatchTaskState.MissingBatchTask, alternateSystemLogItem: null, (tesTask, batchInfo, ct) => enableBatchAutopool ? DeleteBatchJobAndRequeueTaskAsync(tesTask, batchInfo, ct) : AddBatchTaskAsync(tesTask, ct)),
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

        private Task DeleteBatchJobOrTaskAsync(TesTask tesTask, PoolInformation poolInformation, CancellationToken cancellationToken)
            => enableBatchAutopool ? azureProxy.DeleteBatchJobAsync(tesTask.Id, cancellationToken) : poolInformation is null || poolInformation.PoolId is null ? WarnWhenUnableToFindPoolToDeleteTask(tesTask) : azureProxy.DeleteBatchTaskAsync(tesTask.Id, poolInformation, cancellationToken);

        private Task WarnWhenUnableToFindPoolToDeleteTask(TesTask tesTask)
        {
            logger.LogWarning("Unable to delete batch task for task {TesTask} because of missing pool/job information.", tesTask.Id);
            tesTask.SetWarning("Unable to delete batch task because of missing pool/job information.");
            return Task.CompletedTask;
        }

        private async Task DeleteBatchTaskAndOrJobAndOrPoolIfExists(IAzureProxy azureProxy, TesTask tesTask, CombinedBatchTaskInfo batchInfo, CancellationToken cancellationToken)
        {
            var batchDeletionExceptions = new List<Exception>();

            try
            {
                await DeleteBatchJobOrTaskAsync(tesTask, batchInfo.Pool, cancellationToken);
            }
            catch (Exception exc)
            {
                logger.LogError(exc, $"Exception deleting batch task or job with tesTask.Id: {tesTask?.Id}");
                batchDeletionExceptions.Add(exc);
            }

            if (enableBatchAutopool)
            {
                try
                {
                    await azureProxy.DeleteBatchPoolIfExistsAsync(tesTask.Id, cancellationToken);
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
        public IAsyncEnumerable<CloudPool> GetCloudPools(CancellationToken cancellationToken)
            => azureProxy.GetActivePoolsAsync(batchPrefix);

        /// <inheritdoc/>
        public async Task LoadExistingPoolsAsync(CancellationToken cancellationToken)
        {
            if (!enableBatchAutopool)
            {
                await foreach (var cloudPool in GetCloudPools(cancellationToken))
                {
                    try
                    {
                        var batchPool = _batchPoolFactory.CreateNew();
                        await batchPool.AssignPoolAsync(cloudPool, cancellationToken);
                    }
                    catch (Exception exc)
                    {
                        logger.LogError(exc, "When retrieving previously created batch pools and jobs, there were one or more failures when trying to access batch pool {PoolId} or its associated job.", cloudPool.Id);
                    }
                }
            }
        }

        /// <inheritdoc/>
        public async Task UploadTaskRunnerIfNeeded(CancellationToken cancellationToken)
        {
            var blobUri = new Uri(await storageAccessProvider.GetInternalTesBlobUrlAsync(NodeTaskRunnerFilename, cancellationToken));
            var blobProperties = await azureProxy.GetBlobPropertiesAsync(blobUri, cancellationToken);
            if (!(await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, $"scripts/{NodeTaskRunnerMD5HashFilename}"), cancellationToken)).Trim().Equals(blobProperties?.ContentMD5, StringComparison.OrdinalIgnoreCase))
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

        /// <summary>
        /// Determines if the <see cref="Tes.Models.TesInput"/> file is a Cromwell command script
        /// </summary>
        /// <param name="inputFile"><see cref="Tes.Models.TesInput"/> file</param>
        /// <returns>True if the file is a Cromwell command script</returns>
        private static bool IsCromwellCommandScript(TesInput inputFile)
            // See https://github.com/broadinstitute/cromwell/blob/17efd599d541a096dc5704991daeaefdd794fefd/supportedBackends/tes/src/main/scala/cromwell/backend/impl/tes/TesTask.scala#L58
            => (inputFile.Name?.Equals("commandScript") ?? false) && (inputFile.Description?.EndsWith(".commandScript") ?? false) && inputFile.Type == TesFileType.FILEEnum && inputFile.Path.EndsWith($"/{CromwellScriptFileName}");

        /// <summary>
        /// Verifies existence and translates local file URLs to absolute paths (e.g. file:///tmp/cwl_temp_dir_8026387118450035757/args.py becomes /tmp/cwl_temp_dir_8026387118450035757/args.py)
        /// Only considering files in /cromwell-tmp because that is the only local directory mapped from Cromwell container
        /// </summary>
        /// <param name="fileUri">File URI</param>
        /// <param name="localPath">Local path</param>
        /// <returns></returns>
        private bool TryGetCromwellTmpFilePath(string fileUri, out string localPath)
        {
            localPath = Uri.TryCreate(fileUri, UriKind.Absolute, out var uri) && uri.IsFile && uri.AbsolutePath.StartsWith("/cromwell-tmp/") && azureProxy.LocalFileExists(uri.AbsolutePath) ? uri.AbsolutePath : null;

            return localPath is not null;
        }

        /// <summary>
        /// Adds a new Azure Batch pool/job/task for the given <see cref="TesTask"/>
        /// </summary>
        /// <param name="tesTask">The <see cref="TesTask"/> to schedule on Azure Batch</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>A task to await</returns>
        private async Task AddBatchTaskAsync(TesTask tesTask, CancellationToken cancellationToken)
        {
            PoolInformation poolInformation = null;
            string poolKey = null;

            try
            {
                var virtualMachineInfo = await GetVmSizeAsync(tesTask, cancellationToken);

                var containerMetadata = await GetContainerConfigurationIfNeededAsync(tesTask, cancellationToken);
                (poolKey, var displayName) = enableBatchAutopool ? default : GetPoolKey(tesTask, virtualMachineInfo, containerMetadata.ContainerConfiguration, cancellationToken);
                await quotaVerifier.CheckBatchAccountQuotasAsync(virtualMachineInfo, needPoolOrJobQuotaCheck: enableBatchAutopool || !IsPoolAvailable(poolKey), needCoresUtilizationQuotaCheck: enableBatchAutopool, cancellationToken: cancellationToken);

                var tesTaskLog = tesTask.AddTesTaskLog();
                tesTaskLog.VirtualMachineInfo = virtualMachineInfo;
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
                if (enableBatchAutopool)
                {
                    jobOrTaskId = await azureProxy.GetNextBatchJobIdAsync(tesTask.Id, cancellationToken);
                    poolInformation = await CreateAutoPoolModePoolInformation(
                        poolSpecification: await GetPoolSpecification(
                        vmSize: virtualMachineInfo.VmSize,
                        autoscaled: false,
                        preemptable: virtualMachineInfo.LowPriority,
                        nodeInfo: useGen2.GetValueOrDefault() ? gen2BatchNodeInfo : gen1BatchNodeInfo,
                        containerConfiguration: containerMetadata.ContainerConfiguration,
                        encryptionAtHostSupported: virtualMachineInfo.EncryptionAtHostSupported,
                        cancellationToken: cancellationToken),
                    tesTaskId: tesTask.Id,
                    jobId: jobOrTaskId,
                    cancellationToken: cancellationToken,
                    identityResourceIds: identities);
                }
                else
                {
                    poolInformation = (await GetOrAddPoolAsync(
                        key: poolKey,
                        isPreemptable: virtualMachineInfo.LowPriority,
                        modelPoolFactory: async (id, ct) => ConvertPoolSpecificationToModelsPool(
                            name: id,
                            displayName: displayName,
                            poolIdentity: GetBatchPoolIdentity(identities.ToArray()),
                            pool: await GetPoolSpecification(
                                vmSize: virtualMachineInfo.VmSize,
                                autoscaled: true,
                                preemptable: virtualMachineInfo.LowPriority,
                                nodeInfo: useGen2.GetValueOrDefault() ? gen2BatchNodeInfo : gen1BatchNodeInfo,
                                containerConfiguration: containerMetadata.ContainerConfiguration,
                                encryptionAtHostSupported: virtualMachineInfo.EncryptionAtHostSupported,
                                cancellationToken: cancellationToken)),
                        cancellationToken: cancellationToken)
                        ).Pool;
                    jobOrTaskId = $"{tesTask.Id}-{tesTask.Logs.Count}";
                }

                tesTask.PoolId = poolInformation.PoolId;
                var cloudTask = await ConvertTesTaskToBatchTaskAsync(enableBatchAutopool ? tesTask.Id : jobOrTaskId, tesTask, containerMetadata.IsPublic, cancellationToken);
                logger.LogInformation($"Creating batch task for TES task {tesTask.Id}. Using VM size {virtualMachineInfo.VmSize}.");

                if (enableBatchAutopool)
                {
                    await azureProxy.CreateAutoPoolModeBatchJobAsync(jobOrTaskId, cloudTask, poolInformation, cancellationToken);
                }
                else
                {
                    await azureProxy.AddBatchTaskAsync(tesTask.Id, cloudTask, poolInformation, cancellationToken);
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
                if (enableBatchAutopool && poolInformation?.AutoPoolSpecification is not null)
                {
                    await azureProxy.DeleteBatchPoolIfExistsAsync(tesTask.Id, cancellationToken);
                }
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
            var azureBatchJobAndTaskState = await azureProxy.GetBatchJobAndTaskStateAsync(tesTask, enableBatchAutopool, cancellationToken);

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
                if (enableBatchAutopool)
                {
                    _ = ProcessStartTaskFailure((await azureProxy.ListComputeNodesAsync(azureBatchJobAndTaskState.Pool.PoolId, new ODATADetailLevel { FilterClause = "state eq 'starttaskfailed'", SelectClause = "id,startTaskInfo" }).FirstOrDefaultAsync(cancellationToken: cancellationToken))?.StartTaskInformation?.FailureInformation);
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
                                    SystemLogItems = ConvertNodeErrorsToSystemLogItems(azureBatchJobAndTaskState),
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
                        var metrics = await GetBatchNodeMetricsAndCromwellResultCodeAsync(tesTask, cancellationToken);

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
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>True if the TES task was changed.</returns>
        private ValueTask<bool> HandleTesTaskTransitionAsync(TesTask tesTask, CombinedBatchTaskInfo combinedBatchTaskInfo, CancellationToken cancellationToken)
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

            => (tesTaskStateTransitions
                .FirstOrDefault(m => (m.Condition is null || m.Condition(tesTask)) && (m.CurrentBatchTaskState is null || m.CurrentBatchTaskState == combinedBatchTaskInfo.BatchTaskState))
                ?.ActionAsync(tesTask, combinedBatchTaskInfo, cancellationToken) ?? ValueTask.FromResult(false));

        /// <summary>
        /// Returns job preparation and main Batch tasks that represents the given <see cref="TesTask"/>
        /// </summary>
        /// <param name="taskId">The Batch Task Id</param>
        /// <param name="task">The <see cref="TesTask"/></param>
        /// <param name="isPublic">Indicates which container images are public.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>Job preparation and main Batch tasks</returns>
        private async Task<CloudTask> ConvertTesTaskToBatchTaskAsync(string taskId, TesTask task, (bool ExecutorImage, bool DockerInDockerImage, bool CromwellDrsImage) isPublic, CancellationToken cancellationToken)
        {
            var poolHasContainerConfig = !(isPublic.ExecutorImage && isPublic.DockerInDockerImage && isPublic.CromwellDrsImage);
            var cromwellExecutionDirectoryUrl = GetCromwellExecutionDirectoryPathAsUrl(task);
            var isCromwell = cromwellExecutionDirectoryUrl is not null;

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

            var metricsName = "metrics.txt";

            var additionalInputFiles = new List<TesInput>();
            // TODO: Cromwell bug: Cromwell command write_tsv() generates a file in the execution directory, for example execution/write_tsv_3922310b441805fc43d52f293623efbc.tmp. These are not passed on to TES inputs.
            // WORKAROUND: Get the list of files in the execution directory and add them to task inputs.
            // TODO: Verify whether this workaround is still needed.
            if (isCromwell)
            {
                if (!Uri.TryCreate(cromwellExecutionDirectoryUrl, UriKind.Absolute, out _))
                {
                    cromwellExecutionDirectoryUrl = $"/{cromwellExecutionDirectoryUrl}";
                }

                var executionDirectoryUri = await storageAccessProvider.MapLocalPathToSasUrlAsync(cromwellExecutionDirectoryUrl, cancellationToken, getContainerSas: true);
                if (executionDirectoryUri is not null)
                {
                    var blobsInExecutionDirectory = (await azureProxy.ListBlobsAsync(new Uri(executionDirectoryUri), cancellationToken)).ToList();
                    var scriptFile = blobsInExecutionDirectory.FirstOrDefault(b => b.Name.EndsWith($"/{CromwellScriptFileName}"));

                    if (scriptFile is not null)
                    {
                        blobsInExecutionDirectory.Remove(scriptFile);
                        var cromwellExecutionDirectory = Path.GetDirectoryName(scriptFile.Name);
                        additionalInputFiles = await blobsInExecutionDirectory
                            .Select(b => (Path: $"{cromwellExecutionDirectory}/{Path.GetFileName(b.Name)}", b.Uri))
                            .ToAsyncEnumerable()
                            .SelectAwait(async b => new TesInput { Path = b.Path, Url = await storageAccessProvider.MapLocalPathToSasUrlAsync(b.Uri.AbsoluteUri, cancellationToken, getContainerSas: true), Name = Path.GetFileName(b.Path), Type = TesFileType.FILEEnum })
                            .ToListAsync(cancellationToken);
                    }
                }
            }

            var filesToDownload = await Task.WhenAll(
                inputFiles
                .Except(drsInputFiles) // do not attempt to download DRS input files since the cromwell-drs-localizer will
                .Where(f => f?.Streamable == false) // Don't download files where localization_optional is set to true in WDL (corresponds to "Streamable" property being true on TesInput)
                .Union(additionalInputFiles)
                .Select(async f => await GetTesInputFileUrlAsync(f, task, queryStringsToRemoveFromLocalFilePaths, cancellationToken)));

            var downloadFilesScriptContent = new NodeTask
            {
                MetricsFilename = metricsName,
                InputsMetricsFormat = "FileDownloadSizeInBytes={Size}",
                Inputs = filesToDownload.Select(f => new FileInput { SourceUrl = f.Url, Path = LocalizeLocalPath(f.Path), SasStrategy = SasResolutionStrategy.None }).ToList()
            };

            var filesToUpload = Array.Empty<TesOutput>();

            if (task.Outputs?.Count > 0)
            {
                filesToUpload = await Task.WhenAll(
                task.Outputs?.Select(async f =>
                        new TesOutput
                        {
                            Path = f.Path,
                            Url = await storageAccessProvider.MapLocalPathToSasUrlAsync(f.Url, cancellationToken, getContainerSas: true) ?? throw new TesException("InvalidOutputFilePath", $"Unsupported output URL '{f.Url}' for task Id {taskId}. Must start with 'http', '{CromwellPathPrefix}' or use '/accountName/containerName/blobName' pattern where TES service has Contributor access to the storage account."),
                            Name = f.Name,
                            Type = f.Type
                        }));
            }

            // Ignore missing stdout/stderr files. CWL workflows have an issue where if the stdout/stderr are redirected, they are still listed in the TES outputs
            // Ignore any other missing files and directories. WDL tasks can have optional output files.
            // Implementation: do not set Required to True (it defaults to False)
            var uploadFilesScriptContent = new NodeTask
            {
                MetricsFilename = metricsName,
                OutputsMetricsFormat = "FileUploadSizeInBytes={Size}",
                Outputs = filesToUpload.Select(f => new FileOutput { TargetUrl = f.Url, Path = LocalizeLocalPath(f.Path), FileType = ConvertFileType(f.Type), SasStrategy = SasResolutionStrategy.None, PathPrefix = f.PathPrefix }).ToList()
            };

            var executor = task.Executors.First();

            var volumeMountsOption = String.Join(" ", inputFiles
                .Union(additionalInputFiles)
                .Select(f => f.Path)
                .Concat(filesToUpload.Select(f => f.Path))
                .Select(p => p.Split("/", StringSplitOptions.RemoveEmptyEntries)[0])
                .Distinct(StringComparer.Ordinal)
                .Select(s => $"-v $AZ_BATCH_TASK_WORKING_DIR/wd/{s}:/{s}"));

            var workdirOption = string.IsNullOrWhiteSpace(executor.Workdir) ? string.Empty : $"--workdir {executor.Workdir} ";

            var sb = new StringBuilder();

            sb.AppendLinuxLine($"write_kv() {{ echo \"$1=$2\" >> $AZ_BATCH_TASK_WORKING_DIR/metrics.txt; }} && \\");  // Function that appends key=value pair to metrics.txt file
            sb.AppendLinuxLine($"write_ts() {{ write_kv $1 $(date -Iseconds); }} && \\");    // Function that appends key=<current datetime> to metrics.txt file
            sb.AppendLinuxLine($"mkdir -p $AZ_BATCH_TASK_WORKING_DIR/wd && \\");

            if (isPublic.DockerInDockerImage)
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

            if (isPublic.ExecutorImage)
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

            var uploadMetricsScriptSasUrl = await storageAccessProvider.GetInternalTesTaskBlobUrlAsync(task, UploadMetricsScriptFileName, cancellationToken);
            var metricsSasUrl = await storageAccessProvider.GetInternalTesTaskBlobUrlAsync(task, metricsName, cancellationToken);
            var uploadMetricsScriptContent = new NodeTask
            {
                Outputs = new List<FileOutput>() { new FileOutput { Path = metricsName, TargetUrl = metricsSasUrl, FileType = FileType.File, SasStrategy = SasResolutionStrategy.None } }
            };

            sb.AppendLinuxLine($"write_ts DownloadStart && \\");
            sb.AppendLinuxLine($"./{NodeTaskRunnerFilename} download --file {DownloadFilesScriptFileName} && \\");
            sb.AppendLinuxLine($"write_ts DownloadEnd && \\");
            sb.AppendLinuxLine($"chmod -R o+rwx $AZ_BATCH_TASK_WORKING_DIR/wd && \\");
            sb.AppendLinuxLine($"export TES_TASK_WD=$AZ_BATCH_TASK_WORKING_DIR/wd && \\");
            sb.AppendLinuxLine($"write_ts ExecutorStart && \\");
            sb.AppendLinuxLine($"docker run --rm {volumeMountsOption} --entrypoint= {workdirOption}{executor.Image} {executor.Command[0]} {string.Join(" ", executor.Command.Skip(1).Select(BashWrapShellArgument))} && \\");
            sb.AppendLinuxLine($"write_ts ExecutorEnd && \\");
            sb.AppendLinuxLine($"write_ts UploadStart && \\");
            sb.AppendLinuxLine($"./{NodeTaskRunnerFilename} upload --file {UploadFilesScriptFileName} && \\");
            sb.AppendLinuxLine($"write_ts UploadEnd && \\");
            sb.AppendLinuxLine($"/bin/bash -c 'disk=( `df -k $AZ_BATCH_TASK_WORKING_DIR | tail -1` ) && echo DiskSizeInKiB=${{disk[1]}} >> $AZ_BATCH_TASK_WORKING_DIR/metrics.txt && echo DiskUsedInKiB=${{disk[2]}} >> $AZ_BATCH_TASK_WORKING_DIR/metrics.txt' && \\");
            sb.AppendLinuxLine($"write_kv VmCpuModelName \"$(cat /proc/cpuinfo | grep -m1 name | cut -f 2 -d ':' | xargs)\" && \\");
            sb.AppendLinuxLine($"./{NodeTaskRunnerFilename} upload --file {UploadMetricsScriptFileName}");

            var nodeTaskRunnerSasUrl = await storageAccessProvider.GetInternalTesBlobUrlAsync(NodeTaskRunnerFilename, cancellationToken);
            var batchScriptSasUrl =
                await storageAccessProvider.GetInternalTesTaskBlobUrlAsync(task, BatchScriptFileName,
                    cancellationToken);
            var downloadFilesScriptUrl = await storageAccessProvider.GetInternalTesTaskBlobUrlAsync(task, DownloadFilesScriptFileName, cancellationToken);
            var uploadFilesScriptSasUrl = await storageAccessProvider.GetInternalTesTaskBlobUrlAsync(task, UploadFilesScriptFileName, cancellationToken);

            var tesInternalDirectorySasUrl = await storageAccessProvider.GetInternalTesTaskBlobUrlAsync(task, blobPath: string.Empty, cancellationToken);

            await storageAccessProvider.UploadBlobAsync(new Uri(downloadFilesScriptUrl), SerializeNodeTask(downloadFilesScriptContent), cancellationToken);
            await storageAccessProvider.UploadBlobAsync(new Uri(uploadFilesScriptSasUrl), SerializeNodeTask(uploadFilesScriptContent), cancellationToken);
            await storageAccessProvider.UploadBlobAsync(new Uri(uploadMetricsScriptSasUrl), SerializeNodeTask(uploadMetricsScriptContent), cancellationToken);
            await storageAccessProvider.UploadBlobAsync(new Uri(batchScriptSasUrl), sb.ToString(), cancellationToken);

            var batchRunCommand = enableBatchAutopool
                ? $"/bin/bash -c chmod u+x ./{NodeTaskRunnerFilename} && /bin/bash $AZ_BATCH_TASK_WORKING_DIR/{BatchScriptFileName}"
                : $"/bin/bash -c \"{MungeBatchScript()}\"";

            var cloudTask = new CloudTask(taskId, batchRunCommand)
            {
                UserIdentity = new UserIdentity(new AutoUserSpecification(elevationLevel: ElevationLevel.Admin, scope: AutoUserScope.Pool)),
                ResourceFiles = new List<ResourceFile>
                {
                    ResourceFile.FromUrl(nodeTaskRunnerSasUrl, NodeTaskRunnerFilename),
                    ResourceFile.FromUrl(batchScriptSasUrl, BatchScriptFileName),
                    ResourceFile.FromUrl(downloadFilesScriptUrl, DownloadFilesScriptFileName),
                    ResourceFile.FromUrl(uploadFilesScriptSasUrl, UploadFilesScriptFileName),
                    ResourceFile.FromUrl(uploadMetricsScriptSasUrl, UploadMetricsScriptFileName),
                },
                OutputFiles = new List<OutputFile>
                {
                    new OutputFile(
                        "../std*.txt",
                        new OutputFileDestination(new(tesInternalDirectorySasUrl)),
                        new OutputFileUploadOptions(OutputFileUploadCondition.TaskCompletion))
                }
            };

            if (poolHasContainerConfig)
            {
                // If the executor image is private, and in order to run multiple containers in the main task, the image has to be downloaded via pool ContainerConfiguration.
                // This also requires that the main task runs inside a container. So we run the "docker" container that in turn runs other containers.
                // If the executor image is public, there is no need for pool ContainerConfiguration and task can run normally, without being wrapped in a docker container.
                // Volume mapping for docker.sock below allows the docker client in the container to access host's docker daemon.
                // Remark: Batch provides "-v $AZ_BATCH_NODE_ROOT_DIR:$AZ_BATCH_NODE_ROOT_DIR" for us.
                var containerRunOptions = $"--rm -v /var/run/docker.sock:/var/run/docker.sock ";
                cloudTask.ContainerSettings = new TaskContainerSettings(dockerInDockerImageName, containerRunOptions);
            }

            return cloudTask;

            static string BashWrapShellArgument(string argument)
                => $"'{argument.Replace(@"'", @"'\''")}'";

            static FileType ConvertFileType(TesFileType tesFileType)
                => tesFileType switch
                {
                    TesFileType.FILEEnum => FileType.File,
                    TesFileType.DIRECTORYEnum => FileType.Directory,
                    _ => throw new ArgumentOutOfRangeException(nameof(tesFileType)),
                };

            // Yes, this looks "Windowsy", while all our executors run on Linux. Environment.ExpandEnvironmentVariables requires environment variables to be delimited by '%' no matter the platform.
            static string LocalizeLocalPath(string path)
                => $"%AZ_BATCH_TASK_WORKING_DIR%/wd{path}";

            static string SerializeNodeTask(NodeTask task)
                => JsonConvert.SerializeObject(task, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore, DefaultValueHandling = DefaultValueHandling.Ignore });

            string MungeBatchScript()
                => string.Join("\n", taskRunScriptContent)
                    .Replace(@"{CleanupScriptLines}", string.Join("\n", poolHasContainerConfig ? MungeCleanupScriptForContainerConfig(taskCleanupScriptContent) : MungeCleanupScript(taskCleanupScriptContent)))
                    .Replace(@"{BatchScriptPath}", $"$AZ_BATCH_TASK_WORKING_DIR/{BatchScriptFileName}")
                    .Replace(@"{TaskExecutor}", executor.Image)
                    .Replace(@"{ExecutionPathPrefix}", "wd")
                    .Replace("\"", "\\\"");

            static IEnumerable<string> MungeCleanupScript(IEnumerable<string> content)
            {
                return content.Select((line, index) => $"echo '{line}' {Redirect(index)} ../clean-executor.sh");

                static string Redirect(int index)
                    => index == 0 ? ">" : ">>";
            }

            static IEnumerable<string> MungeCleanupScriptForContainerConfig(IEnumerable<string> content)
                => MungeCleanupScript(content.Where(line => !line.Contains(@"{TaskExecutor}")));
        }

        /// <summary>
        /// Converts the input file URL into proper http URL with SAS token, ready for batch to download.
        /// Removes the query strings from the input file path and the command script content.
        /// Uploads the file if content is provided.
        /// </summary>
        /// <param name="inputFile"><see cref="TesInput"/> file</param>
        /// <param name="task">TES task</param>
        /// <param name="queryStringsToRemoveFromLocalFilePaths">Query strings to remove from local file paths</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>List of modified <see cref="TesInput"/> files</returns>
        private async Task<TesInput> GetTesInputFileUrlAsync(TesInput inputFile, TesTask task, List<string> queryStringsToRemoveFromLocalFilePaths, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(inputFile.Path) || !inputFile.Path.StartsWith("/"))
            {
                throw new TesException("InvalidInputFilePath", $"Unsupported input path '{inputFile.Path}' for task Id {task.Id}. Must start with '/'.");
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
                inputFileUrl = await storageAccessProvider.GetInternalTesTaskBlobUrlAsync(task, Guid.NewGuid().ToString(), cancellationToken);
                var content = inputFile.Content ?? await storageAccessProvider.DownloadBlobAsync(inputFile.Url, cancellationToken);
                content = IsCromwellCommandScript(inputFile) ? RemoveQueryStringsFromLocalFilePaths(content, queryStringsToRemoveFromLocalFilePaths) : content;

                await storageAccessProvider.UploadBlobAsync(new Uri(inputFileUrl), content, cancellationToken);
            }
            else if (TryGetCromwellTmpFilePath(inputFile.Url, out var localPath))
            {
                var storageFileName = $"/{GetStorageUploadPath(task)}/{Guid.NewGuid()}";
                inputFileUrl = await storageAccessProvider.MapLocalPathToSasUrlAsync(storageFileName, cancellationToken);
                await storageAccessProvider.UploadBlobFromFileAsync(storageFileName, localPath, cancellationToken);
            }
            else if (await storageAccessProvider.IsPublicHttpUrlAsync(inputFile.Url, cancellationToken))
            {
                inputFileUrl = inputFile.Url;
            }
            else
            {
                // Convert file:///account/container/blob paths to /account/container/blob
                var url = Uri.TryCreate(inputFile.Url, UriKind.Absolute, out var tempUrl) && tempUrl.IsFile ? tempUrl.AbsolutePath : inputFile.Url;
                inputFileUrl = (await storageAccessProvider.MapLocalPathToSasUrlAsync(url, cancellationToken)) ?? throw new TesException("InvalidInputFilePath", $"Unsupported input URL '{inputFile.Url}' for task Id {task.Id}. Must start with 'http', '{CromwellPathPrefix}' or use '/accountName/containerName/blobName' pattern where TES service has Contributor access to the storage account.");
            }

            var path = RemoveQueryStringsFromLocalFilePaths(inputFile.Path, queryStringsToRemoveFromLocalFilePaths);
            return new TesInput { Url = inputFileUrl, Path = path };
        }

        /// <summary>
        /// Constructs a universal Azure Start Task instance if needed
        /// </summary>
        /// <param name="machineConfiguration">A <see cref="VirtualMachineConfiguration"/> describing the OS of the pool's nodes.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        /// <remarks>This method also mitigates errors associated with docker daemons that are not configured to place their filestsystem assets on the data drive. Errors attempting to do so are ignored.</remarks>
        private async Task<StartTask> StartTaskIfNeeded(VirtualMachineConfiguration machineConfiguration, CancellationToken cancellationToken)
        {
            var installJq = machineConfiguration.NodeAgentSkuId switch
            {
                var s when s.StartsWith("batch.node.ubuntu ") => "sudo apt-get install -y jq",
                var s when s.StartsWith("batch.node.centos ") => "sudo yum install epel-release -y && sudo yum update -y && sudo yum install jq -y",
                _ => throw new InvalidOperationException($"Unrecognized OS. Please send open an issue @ 'https://github.com/microsoft/ga4gh-tes/issues' with this message. ({machineConfiguration.NodeAgentSkuId})")
            };

            var commandPart1 = @"/usr/bin/bash -c 'trap ""echo Error trapped; exit 0"" ERR; sudo touch tmp2.json && sudo cp /etc/docker/daemon.json tmp1.json && sudo chmod a+w tmp?.json && if fgrep -q ""$(dirname ""$AZ_BATCH_NODE_ROOT_DIR"")/docker"" tmp1.json; then ";
            var commandPart2 = @" && jq \.\[\""data-root\""\]=\""""$(dirname ""$AZ_BATCH_NODE_ROOT_DIR"")/docker""\"" tmp1.json >> tmp2.json && sudo mv tmp2.json /etc/docker/daemon.json && sudo systemctl restart docker; fi'";

            var startTask = new StartTask
            {
                CommandLine = commandPart1 + installJq + commandPart2,
                UserIdentity = new UserIdentity(new AutoUserSpecification(elevationLevel: ElevationLevel.Admin, scope: AutoUserScope.Pool)),
            };

            if (!string.IsNullOrWhiteSpace(globalStartTaskPath))
            {
                var startTaskSasUrl = enableBatchAutopool
                    ? await storageAccessProvider.MapLocalPathToSasUrlAsync(globalStartTaskPath, cancellationToken)
                    : await storageAccessProvider.MapLocalPathToSasUrlAsync(globalStartTaskPath, cancellationToken, sasTokenDuration: BatchPoolService.RunInterval.Multiply(2).Add(poolLifetime).Add(TimeSpan.FromMinutes(15)));

                if (await azureProxy.BlobExistsAsync(new Uri(startTaskSasUrl), cancellationToken))
                {
                    startTask.ResourceFiles = new List<ResourceFile> { ResourceFile.FromUrl(startTaskSasUrl, StartTaskScriptFilename) };
                    startTask.CommandLine = $"({startTask.CommandLine}) && ./{StartTaskScriptFilename}";
                }
            }

            return startTask;
        }

        /// <summary>
        /// Constructs an Azure Batch Container Configuration instance
        /// </summary>
        /// <param name="tesTask">The <see cref="TesTask"/> to schedule on Azure Batch</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        private async ValueTask<(ContainerConfiguration ContainerConfiguration, (bool ExecutorImage, bool DockerInDockerImage, bool CromwellDrsImage) IsPublic)> GetContainerConfigurationIfNeededAsync(TesTask tesTask, CancellationToken cancellationToken)
        {
            var drsImageNeeded = tesTask.Inputs?.Any(i => i?.Url?.StartsWith("drs://") ?? false) ?? false;
            // TODO: Support for multiple executors. Cromwell has single executor per task.
            var executorImage = tesTask.Executors.First().Image;

            var dockerInDockerIsPublic = true;
            var executorImageIsPublic = containerRegistryProvider.IsImagePublic(executorImage);
            var cromwellDrsIsPublic = drsImageNeeded ? containerRegistryProvider.IsImagePublic(cromwellDrsLocalizerImageName) : true;

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
                    var containerRegistryInfo = await containerRegistryProvider.GetContainerRegistryInfoAsync(executorImage, cancellationToken);
                    if (containerRegistryInfo is not null)
                    {
                        result.ContainerRegistries.Add(new(
                            userName: containerRegistryInfo.Username,
                            registryServer: containerRegistryInfo.RegistryServer,
                            password: containerRegistryInfo.Password));
                    }
                }

                if (!cromwellDrsIsPublic)
                {
                    var containerRegistryInfo = await containerRegistryProvider.GetContainerRegistryInfoAsync(cromwellDrsLocalizerImageName, cancellationToken);
                    if (containerRegistryInfo is not null && !result.ContainerRegistries.Any(registry => registry.RegistryServer == containerRegistryInfo.RegistryServer))
                    {
                        result.ContainerRegistries.Add(new(
                            userName: containerRegistryInfo.Username,
                            registryServer: containerRegistryInfo.RegistryServer,
                            password: containerRegistryInfo.Password));
                    }
                }

                if (result.ContainerRegistries.Count != 0)
                {
                    var containerRegistryInfo = await containerRegistryProvider.GetContainerRegistryInfoAsync(dockerInDockerImageName, cancellationToken);
                    dockerInDockerIsPublic = containerRegistryInfo is null;
                    if (containerRegistryInfo is not null && !result.ContainerRegistries.Any(registry => registry.RegistryServer == containerRegistryInfo.RegistryServer))
                    {
                        result.ContainerRegistries.Add(new(
                            userName: containerRegistryInfo.Username,
                            registryServer: containerRegistryInfo.RegistryServer,
                            password: containerRegistryInfo.Password));
                    }
                }
            }

            return result is null || result.ContainerRegistries.Count == 0 ? (default, (true, true, true)) : (new()
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
            }, (executorImageIsPublic, dockerInDockerIsPublic, cromwellDrsIsPublic));
        }

        /// <summary>
        /// Constructs either an <see cref="AutoPoolSpecification"/> or a new pool in the batch account ready for a job to be attached.
        /// </summary>
        /// <param name="poolSpecification"></param>
        /// <param name="tesTaskId"></param>
        /// <param name="jobId"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="identityResourceIds"></param>
        /// <remarks>If <paramref name="identityResourceIds"/> is provided, <paramref name="jobId"/> must also be provided.<br/>This method does not support autscaled pools.</remarks>
        /// <returns>An <see cref="PoolInformation"/></returns>
        private async Task<PoolInformation> CreateAutoPoolModePoolInformation(PoolSpecification poolSpecification, string tesTaskId, string jobId, CancellationToken cancellationToken, IEnumerable<string> identityResourceIds = null)
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
                    IsPreemptable(), cancellationToken);

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
        /// <param name="encryptionAtHostSupported">VM supports encryption at host.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns><see cref="PoolSpecification"/></returns>
        /// <remarks>We use the PoolSpecification for both the namespace of all the constituent parts and for the fact that it allows us to configure shared and autopools using the same code.</remarks>
        private async ValueTask<PoolSpecification> GetPoolSpecification(string vmSize, bool autoscaled, bool preemptable, BatchNodeInfo nodeInfo, ContainerConfiguration containerConfiguration, bool encryptionAtHostSupported, CancellationToken cancellationToken)
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
                StartTask = await StartTaskIfNeeded(vmConfig, cancellationToken),
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

            if (!string.IsNullOrEmpty(batchNodesSubnetId))
            {
                poolSpecification.NetworkConfiguration = new()
                {
                    PublicIPAddressConfiguration = new PublicIPAddressConfiguration(disableBatchNodesPublicIpAddress ? IPAddressProvisioningType.NoPublicIPAddresses : IPAddressProvisioningType.BatchManaged),
                    SubnetId = batchNodesSubnetId
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

            static BatchModels.DiskEncryptionConfiguration ConvertDiskEncryptionConfiguration(DiskEncryptionConfiguration diskEncryptionConfiguration)
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
