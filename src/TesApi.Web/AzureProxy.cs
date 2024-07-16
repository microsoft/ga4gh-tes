// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Core;
using Azure.ResourceManager;
using Azure.ResourceManager.Resources;
using Azure.ResourceManager.Storage;
using Azure.Storage.Blobs;
using CommonUtilities;
using CommonUtilities.AzureCloud;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Rest;
using Polly;
using Tes.Models;
using TesApi.Web.Management;
using TesApi.Web.Management.Configuration;
using TesApi.Web.Storage;
using static CommonUtilities.RetryHandler;
using BatchProtocol = Microsoft.Azure.Batch.Protocol;
using BlobModels = Azure.Storage.Blobs.Models;
using CloudTask = Microsoft.Azure.Batch.CloudTask;
using ComputeNodeState = Microsoft.Azure.Batch.Common.ComputeNodeState;
using JobState = Microsoft.Azure.Batch.Common.JobState;
using OnAllTasksComplete = Microsoft.Azure.Batch.Common.OnAllTasksComplete;
using TaskExecutionInformation = Microsoft.Azure.Batch.TaskExecutionInformation;
using TaskState = Microsoft.Azure.Batch.Common.TaskState;

namespace TesApi.Web
{
    /// <summary>
    /// Wrapper for Azure APIs
    /// </summary>
    public partial class AzureProxy : IAzureProxy
    {
        private const char BatchJobAttemptSeparator = '-';
        private readonly AsyncRetryHandlerPolicy batchRetryPolicyWhenJobNotFound;
        private readonly AsyncRetryHandlerPolicy batchRetryPolicyWhenNodeNotReady;

        private readonly ILogger logger;
        private readonly AzureServicesConnectionStringCredentialOptions credentialOptions;
        private readonly BatchProtocol.BatchServiceClient batchServiceClient;
        private readonly BatchClient batchClient;
        private readonly string location;
        private readonly ArmEnvironment armEnvironment;

        /// <summary>
        /// Constructor of AzureProxy
        /// </summary>
        /// <param name="batchAccountOptions">The Azure Batch Account options</param>
        /// <param name="batchAccountInformation">The Azure Batch Account information</param>
        /// <param name="credentialOptions"></param>
        /// <param name="azureCloudConfig"></param>
        /// <param name="retryHandler">Retry builder</param>
        /// <param name="logger">The logger</param>
        /// <exception cref="InvalidOperationException"></exception>
        public AzureProxy(IOptions<BatchAccountOptions> batchAccountOptions, BatchAccountResourceInformation batchAccountInformation, AzureServicesConnectionStringCredentialOptions credentialOptions, AzureCloudConfig azureCloudConfig, RetryPolicyBuilder retryHandler, ILogger<AzureProxy> logger)
        {
            ArgumentNullException.ThrowIfNull(batchAccountOptions);
            ArgumentNullException.ThrowIfNull(batchAccountInformation);
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(credentialOptions);
            ArgumentNullException.ThrowIfNull(retryHandler);
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(azureCloudConfig);

            credentialOptions.AuthorityHost = azureCloudConfig.AuthorityHost;

            this.armEnvironment = azureCloudConfig.ArmEnvironment.Value;
            this.credentialOptions = credentialOptions;
            this.logger = logger;

            if (string.IsNullOrWhiteSpace(batchAccountOptions.Value.AccountName))
            {
                throw new ArgumentException("The batch account name is missing from the the configuration.", nameof(batchAccountOptions));
            }

            batchRetryPolicyWhenJobNotFound = retryHandler.PolicyBuilder
                .OpinionatedRetryPolicy(Policy.Handle<BatchException>(ex => BatchErrorCodeStrings.JobNotFound.Equals(ex.RequestInformation.BatchError.Code, StringComparison.OrdinalIgnoreCase)))
                .WithExceptionBasedWaitWithRetryPolicyOptionsBackup((attempt, exception) => (exception as BatchException)?.RequestInformation?.RetryAfter, backupSkipProvidedIncrements: true)
                .SetOnRetryBehavior(onRetry: LogRetryErrorOnRetryHandler())
                .AsyncBuild();

            batchRetryPolicyWhenNodeNotReady = retryHandler.PolicyBuilder
                .OpinionatedRetryPolicy(Policy.Handle<BatchException>(ex => "NodeNotReady".Equals(ex.RequestInformation.BatchError.Code, StringComparison.OrdinalIgnoreCase)))
                .WithExceptionBasedWaitWithRetryPolicyOptionsBackup((attempt, exception) => (exception as BatchException)?.RequestInformation?.RetryAfter, backupSkipProvidedIncrements: true)
                .SetOnRetryBehavior(onRetry: LogRetryErrorOnRetryHandler())
                .AsyncBuild();

            ServiceClientCredentials serviceClientCredentials = null;

            if (!string.IsNullOrWhiteSpace(batchAccountOptions.Value.AppKey))
            {
                //If the key is provided assume we won't use ARM and the information will be provided via config
                serviceClientCredentials = new BatchProtocol.BatchSharedKeyCredential(
                    batchAccountOptions.Value.AccountName, batchAccountOptions.Value.AppKey);
                location = batchAccountOptions.Value.Region;
            }
            else
            {
                location = batchAccountInformation.Region;
                var credentials = new AzureServicesConnectionStringCredential(credentialOptions);
                serviceClientCredentials = new TokenCredentials(new BatchProtocol.BatchTokenProvider(async () =>
                    (await credentials.GetTokenAsync(new TokenRequestContext(
                        [azureCloudConfig.BatchUrl.TrimEnd('/') + "/.default"],
                        tenantId: azureCloudConfig.Authentication.Tenant), CancellationToken.None)).Token));
            }

            batchServiceClient = new(serviceClientCredentials) { BatchUrl = batchAccountInformation.BaseUrl };
            batchClient = BatchClient.Open(batchServiceClient);
        }

        /// <summary>
        /// A logging retry handler.
        /// </summary>
        /// <returns><see cref="OnRetryHandler"/></returns>
        private OnRetryHandler LogRetryErrorOnRetryHandler()
            => new((exception, timeSpan, retryCount, correlationId, caller) =>
            {
                var requestId = (exception as BatchException)?.RequestInformation?.ServiceRequestId ?? "n/a";
                var reason = (exception.InnerException as BatchProtocol.Models.BatchErrorException)?.Response?.ReasonPhrase ?? "n/a";
                logger?.LogError(exception, @"Retrying in {Method}: RetryCount: {RetryCount} RetryCount: {TimeSpan:c} BatchErrorCode: '{BatchErrorCode}', ApiStatusCode '{ApiStatusCode}', Reason: '{ReasonPhrase}' ServiceRequestId: '{ServiceRequestId}', CorrelationId: {CorrelationId:D}",
                    caller, retryCount, timeSpan, (exception as BatchException)?.RequestInformation?.BatchError?.Code ?? "n/a", (exception as BatchException)?.RequestInformation?.HttpStatusCode?.ToString("G") ?? "n/a", reason, requestId, correlationId);
            });


        /// <inheritdoc/>
        public IEnumerable<AzureBatchNodeCount> GetBatchActiveNodeCountByVmSize()
            => batchClient.PoolOperations.ListPools()
                .Select(p => new
                {
                    p.VirtualMachineSize,
                    DedicatedNodeCount = Math.Max(p.TargetDedicatedComputeNodes ?? 0, p.CurrentDedicatedComputeNodes ?? 0),
                    LowPriorityNodeCount = Math.Max(p.TargetLowPriorityComputeNodes ?? 0, p.CurrentLowPriorityComputeNodes ?? 0)
                })
                .GroupBy(x => x.VirtualMachineSize)
                .Select(grp => new AzureBatchNodeCount { VirtualMachineSize = grp.Key, DedicatedNodeCount = grp.Sum(x => x.DedicatedNodeCount), LowPriorityNodeCount = grp.Sum(x => x.LowPriorityNodeCount) });

        /// <inheritdoc/>
        public int GetBatchActivePoolCount()
        {
            var activePoolsFilter = new ODATADetailLevel
            {
                FilterClause = "state eq 'active' or state eq 'deleting'",
                SelectClause = "id"
            };

            return batchClient.PoolOperations.ListPools(activePoolsFilter).ToAsyncEnumerable().CountAsync(CancellationToken.None).AsTask().Result;
        }

        /// <inheritdoc/>
        public int GetBatchActiveJobCount()
        {
            var activeJobsFilter = new ODATADetailLevel
            {
                FilterClause = "state eq 'active' or state eq 'disabling' or state eq 'terminating' or state eq 'deleting'",
                SelectClause = "id"
            };

            return batchClient.JobOperations.ListJobs(activeJobsFilter).ToAsyncEnumerable().CountAsync(CancellationToken.None).AsTask().Result;
        }

        /// <inheritdoc/>
        public async Task CreateBatchJobAsync(string jobId, string poolId, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrEmpty(jobId);

            logger.LogInformation("TES: Creating Batch job {BatchJob}", jobId);
            var job = batchClient.JobOperations.CreateJob(jobId, new() { PoolId = poolId });
            job.OnAllTasksComplete = OnAllTasksComplete.NoAction;
            job.OnTaskFailure = OnTaskFailure.NoAction;

            await job.CommitAsync(cancellationToken: cancellationToken);
            logger.LogInformation("TES: Batch job {BatchJob} committed successfully", jobId);
            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
        }

        /// <inheritdoc/>
        public async Task AddBatchTaskAsync(string tesTaskId, CloudTask cloudTask, string jobId, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrEmpty(jobId);

            logger.LogInformation("TES task: {TesTask} - Adding task to job {BatchJob}", tesTaskId, jobId);
            var job = await batchRetryPolicyWhenJobNotFound.ExecuteWithRetryAsync(ct =>
                batchClient.JobOperations.GetJobAsync(jobId, cancellationToken: ct),
                cancellationToken);

            await job.AddTaskAsync(cloudTask, cancellationToken: cancellationToken);
            logger.LogInformation("TES task: {TesTask} - Added task successfully", tesTaskId);
        }

        /// <inheritdoc/>
        public async Task DeleteBatchJobAsync(string jobId, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrEmpty(jobId);
            logger.LogInformation("Deleting job {BatchJob}", jobId);
            await batchClient.JobOperations.DeleteJobAsync(jobId, cancellationToken: cancellationToken);
        }

        /// <inheritdoc/>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1826:Do not use Enumerable methods on indexable collections", Justification = "FirstOrDefault() is straightforward, the alternative is less clear.")]
        public async Task<AzureBatchJobAndTaskState> GetBatchJobAndTaskStateAsync(TesTask tesTask, CancellationToken cancellationToken)
        {
            try
            {
                string nodeErrorCode = null;
                IEnumerable<string> nodeErrorDetails = null;
                var activeJobWithMissingAutoPool = false;
                ComputeNodeState? nodeState = null;
                TaskState? taskState = null;
                string poolId = null;
                TaskExecutionInformation taskExecutionInformation = null;
                CloudJob job = null;
                var attemptNumber = 0;
                CloudTask batchTask = null;

                ODATADetailLevel jobOrTaskFilter = new(filterClause: $"startswith(id,'{tesTask.Id}{BatchJobAttemptSeparator}')", selectClause: "*");

                if (string.IsNullOrWhiteSpace(tesTask.PoolId))
                {
                    return new AzureBatchJobAndTaskState { JobState = null };
                }

                try
                {
                    job = await batchClient.JobOperations.GetJobAsync(tesTask.PoolId, cancellationToken: cancellationToken);
                }
                catch (BatchException ex) when (ex.InnerException is BatchProtocol.Models.BatchErrorException e && e.Response.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    logger.LogError(ex, @"Failed to get job for TesTask {TesTask}", tesTask.Id);
                    return new AzureBatchJobAndTaskState { JobState = null };
                }

                var taskInfos = await batchClient.JobOperations.ListTasks(tesTask.PoolId, jobOrTaskFilter).ToAsyncEnumerable()
                    .Select(t => new { Task = t, AttemptNumber = int.Parse(t.Id.Split(BatchJobAttemptSeparator)[1]) })
                    .ToListAsync(cancellationToken);

                if (!taskInfos.Any())
                {
                    logger.LogError(@"Failed to get task for TesTask {TesTask}", tesTask.Id);
                }
                else
                {
                    if (taskInfos.Count(t => t.Task.State != TaskState.Completed) > 1)
                    {
                        return new AzureBatchJobAndTaskState { MoreThanOneActiveJobOrTaskFound = true };
                    }

                    var lastTaskInfo = taskInfos.OrderBy(t => t.AttemptNumber).Last();
                    batchTask = lastTaskInfo.Task;
                    attemptNumber = lastTaskInfo.AttemptNumber;
                }

                poolId = job.ExecutionInformation?.PoolId;

                bool ComputeNodePredicate(ComputeNode n) => (n.RecentTasks?.Select(t => t.TaskId) ?? []).Contains(batchTask?.Id);

                var nodeId = string.Empty;

                if (job.State == JobState.Active && poolId is not null)
                {
                    var poolFilter = new ODATADetailLevel
                    {
                        SelectClause = "*"
                    };

                    CloudPool pool;

                    try
                    {
                        pool = await batchClient.PoolOperations.GetPoolAsync(poolId, poolFilter, cancellationToken: cancellationToken);
                    }
                    catch (BatchException ex) when (ex.InnerException is BatchProtocol.Models.BatchErrorException e && e.Response?.StatusCode == System.Net.HttpStatusCode.NotFound)
                    {
                        pool = default;
                    }

                    if (pool is not null)
                    {
                        var node = await pool.ListComputeNodes().ToAsyncEnumerable().FirstOrDefaultAsync(ComputeNodePredicate, cancellationToken);

                        if (node is not null)
                        {
                            nodeId = node.Id;
                            nodeState = node.State;
                            var nodeError = node.Errors?.FirstOrDefault(e => "DiskFull".Equals(e.Code, StringComparison.InvariantCultureIgnoreCase)) ?? node.Errors?.FirstOrDefault(); // Prioritize DiskFull errors
                            nodeErrorCode = nodeError?.Code;
                            nodeErrorDetails = nodeError?.ErrorDetails?.Select(e => e.Value);
                        }
                    }
                    else
                    {
                        if (job.CreationTime.HasValue && DateTime.UtcNow.Subtract(job.CreationTime.Value) > TimeSpan.FromMinutes(30))
                        {
                            activeJobWithMissingAutoPool = true;
                        }
                    }
                }

                if (batchTask is not null)
                {
                    taskState = batchTask.State;
                    taskExecutionInformation = batchTask.ExecutionInformation;
                }

                return new AzureBatchJobAndTaskState
                {
                    MoreThanOneActiveJobOrTaskFound = false,
                    ActiveJobWithMissingAutoPool = activeJobWithMissingAutoPool,
                    AttemptNumber = attemptNumber,
                    NodeErrorCode = nodeErrorCode,
                    NodeErrorDetails = nodeErrorDetails,
                    NodeState = nodeState,
                    JobState = job.State,
                    TaskState = taskState,
                    PoolId = poolId,
                    TaskExecutionResult = taskExecutionInformation?.Result,
                    TaskStartTime = taskExecutionInformation?.StartTime,
                    TaskEndTime = taskExecutionInformation?.EndTime,
                    TaskExitCode = taskExecutionInformation?.ExitCode,
                    TaskFailureInformation = taskExecutionInformation?.FailureInformation,
                    TaskContainerState = taskExecutionInformation?.ContainerInformation?.State,
                    TaskContainerError = taskExecutionInformation?.ContainerInformation?.Error,
                    NodeId = !string.IsNullOrEmpty(nodeId) ? nodeId : null
                };
            }
            catch (Exception ex)
            {
                logger.LogError(ex, @"GetBatchJobAndTaskStateAsync failed for TesTask {TesTask}", tesTask.Id);
                throw;
            }
        }

        /// <inheritdoc/>
        public async Task DeleteBatchTaskAsync(string tesTaskId, string poolId, CancellationToken cancellationToken)
        {
            ODATADetailLevel jobFilter = new(filterClause: $"startswith(id,'{tesTaskId}{BatchJobAttemptSeparator}')", selectClause: "id");
            List<CloudTask> batchTasksToDelete = default;

            try
            {
                batchTasksToDelete = await batchClient.JobOperations.ListTasks(poolId, jobFilter).ToAsyncEnumerable().ToListAsync(cancellationToken);
            }
            catch (BatchException ex) when (ex.InnerException is BatchProtocol.Models.BatchErrorException bee && "JobNotFound".Equals(bee.Body?.Code, StringComparison.InvariantCultureIgnoreCase))
            {
                logger.LogWarning("Job not found for TES task {TesTask}", tesTaskId);
                return; // Task cannot exist if the job is not found.
            }

            if (batchTasksToDelete.Count > 1)
            {
                logger.LogWarning("Found more than one active task for TES task {TesTask}", tesTaskId);
            }

            foreach (var task in batchTasksToDelete)
            {
                logger.LogInformation("Deleting task {BatchTask}", task.Id);
                await batchRetryPolicyWhenNodeNotReady.ExecuteWithRetryAsync(ct => task.DeleteAsync(cancellationToken: ct), cancellationToken);
            }
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<string>> GetActivePoolIdsAsync(string prefix, TimeSpan minAge, CancellationToken cancellationToken = default)
        {
            ODATADetailLevel activePoolsFilter = new(filterClause: $"state eq 'active' and startswith(id, '{prefix}') and creationTime lt DateTime'{DateTime.UtcNow.Subtract(minAge):yyyy-MM-ddTHH:mm:ssZ}'", selectClause: "id");
            return (await batchClient.PoolOperations.ListPools(activePoolsFilter).ToListAsync(cancellationToken)).Select(p => p.Id);
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudPool> GetActivePoolsAsync(string hostName)
        {
            var activePoolsFilter = new ODATADetailLevel
            {
                FilterClause = "state eq 'active'",
                SelectClause = BatchPool.CloudPoolSelectClause
            };

            return batchClient.PoolOperations.ListPools(activePoolsFilter).ToAsyncEnumerable()
                .Where(p => p.Metadata?.Any(metadata => metadata.Name switch
                {
                    BatchScheduler.PoolMetadata => IBatchScheduler.PoolMetadata.Create(metadata.Value).HostName.Equals(hostName, StringComparison.OrdinalIgnoreCase),
                    BatchScheduler.PoolDeprecated => metadata.Value.Equals(hostName, StringComparison.OrdinalIgnoreCase),
                    _ => false
                }) ?? false);
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<string>> GetPoolIdsReferencedByJobsAsync(CancellationToken cancellationToken = default)
            => (await batchClient.JobOperations.ListJobs(new ODATADetailLevel(selectClause: "executionInfo")).ToListAsync(cancellationToken))
                .Where(j => !string.IsNullOrEmpty(j.ExecutionInformation?.PoolId))
                .Select(j => j.ExecutionInformation.PoolId);

        /// <inheritdoc/>
        public Task DeleteBatchComputeNodesAsync(string poolId, IEnumerable<ComputeNode> computeNodes, CancellationToken cancellationToken = default)
            => batchClient.PoolOperations.RemoveFromPoolAsync(poolId, computeNodes, deallocationOption: ComputeNodeDeallocationOption.Requeue, resizeTimeout: TimeSpan.FromMinutes(30), cancellationToken: cancellationToken);

        /// <inheritdoc/>
        public Task<CloudPool> GetBatchPoolAsync(string poolId, CancellationToken cancellationToken = default, DetailLevel detailLevel = default)
            => batchClient.PoolOperations.GetPoolAsync(poolId, detailLevel: detailLevel, cancellationToken: cancellationToken);

        /// <inheritdoc/>
        public Task<CloudJob> GetBatchJobAsync(string jobId, CancellationToken cancellationToken, DetailLevel detailLevel)
            => batchClient.JobOperations.GetJobAsync(jobId, detailLevel, cancellationToken: cancellationToken);

        /// <inheritdoc/>
        public async Task<FullBatchPoolAllocationState> GetFullAllocationStateAsync(string poolId, CancellationToken cancellationToken = default)
        {
            var pool = await batchClient.PoolOperations.GetPoolAsync(poolId, detailLevel: new ODATADetailLevel(selectClause: "allocationState,allocationStateTransitionTime,enableAutoScale,targetLowPriorityNodes,currentLowPriorityNodes,targetDedicatedNodes,currentDedicatedNodes"), cancellationToken: cancellationToken);
            return new(pool.AllocationState, pool.AllocationStateTransitionTime, pool.AutoScaleEnabled, pool.TargetLowPriorityComputeNodes, pool.CurrentLowPriorityComputeNodes, pool.TargetDedicatedComputeNodes, pool.CurrentDedicatedComputeNodes);
        }

        private IAsyncEnumerable<StorageAccountInfo> GetAccessibleStorageAccountsAsync(CancellationToken cancellationToken)
        {
            var azureClient = GetAzureManagementClient();
            return azureClient.GetSubscriptions().SelectMany(s => s.GetStorageAccountsAsync(cancellationToken)).SelectAwaitWithCancellation(async (a, ct) => (await a.GetAsync(cancellationToken: ct)).Value)
                .Select(a => new StorageAccountInfo { Id = a.Id, Name = a.Data.Name, SubscriptionId = a.Id.SubscriptionId, BlobEndpoint = a.Data.PrimaryEndpoints.BlobUri });
        }

        /// <inheritdoc/>
        public async Task<string> GetStorageAccountKeyAsync(StorageAccountInfo storageAccountInfo, CancellationToken cancellationToken)
        {
            try
            {
                ResourceIdentifier storageAccountId = new(storageAccountInfo.Id);
                var azureClient = GetAzureManagementClient().GetResourceGroupResource(ResourceGroupResource.CreateResourceIdentifier(storageAccountId.SubscriptionId, storageAccountId.ResourceGroupName));
                var storageAccount = (await azureClient.GetStorageAccountAsync(storageAccountId.Name, cancellationToken: cancellationToken)).Value;

                return (await storageAccount.GetKeysAsync(cancellationToken: cancellationToken).FirstAsync(key => Azure.ResourceManager.Storage.Models.StorageAccountKeyPermission.Full.Equals(key.Permissions), cancellationToken)).Value;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, $"An exception occurred when getting the storage account key for account {storageAccountInfo.Name}.");
                throw;
            }
        }

        /// <inheritdoc/>
        public Task UploadBlobAsync(Uri blobAbsoluteUri, string content, CancellationToken cancellationToken)
            => new BlobClient(blobAbsoluteUri).UploadAsync(BinaryData.FromString(content), cancellationToken);

        /// <inheritdoc/>
        public Task UploadBlobFromFileAsync(Uri blobAbsoluteUri, string filePath, CancellationToken cancellationToken)
        {
            using var stream = System.IO.File.OpenRead(filePath);
            return new BlobClient(blobAbsoluteUri).UploadAsync(BinaryData.FromStream(stream), cancellationToken);
        }

        /// <inheritdoc/>
        public async Task<string> DownloadBlobAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken)
            => (await new BlobClient(blobAbsoluteUri).DownloadContentAsync(cancellationToken)).Value.Content.ToString();

        /// <inheritdoc/>
        public async Task<bool> BlobExistsAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken)
            => (await new BlobClient(blobAbsoluteUri).ExistsAsync(cancellationToken)).Value;

        /// <inheritdoc/>
        public async Task<BlobModels.BlobProperties> GetBlobPropertiesAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken)
        {
            var blob = new BlobClient(blobAbsoluteUri);

            if ((await blob.ExistsAsync(cancellationToken)).Value)
            {
                return (await blob.GetPropertiesAsync(cancellationToken: cancellationToken)).Value;
            }

            return default;
        }

        /// <inheritdoc/>
        public Task<IEnumerable<BlobModels.BlobItem>> ListBlobsAsync(Uri directoryUri, CancellationToken cancellationToken)
        {
            BlobUriBuilder uriBuilder = new(directoryUri);
            var prefix = uriBuilder.BlobName + "/";
            uriBuilder.BlobName = null;
            BlobContainerClient container = new(uriBuilder.ToUri());

            return Task.FromResult(container.GetBlobsAsync(prefix: prefix, cancellationToken: cancellationToken).ToBlockingEnumerable(cancellationToken));
        }

        /// <inheritdoc />
        public string GetArmRegion()
            => location;

        /// <summary>
        /// Gets an authenticated Azure Client instance
        /// </summary>
        /// <returns>An authenticated Azure Client instance</returns>
        private ArmClient GetAzureManagementClient()
        {
            return new(new AzureServicesConnectionStringCredential(credentialOptions),
                default,
                new ArmClientOptions { Environment = armEnvironment });
        }

        /// <inheritdoc/>
        public async Task<StorageAccountInfo> GetStorageAccountInfoAsync(string storageAccountName, CancellationToken cancellationToken)
            => await GetAccessibleStorageAccountsAsync(cancellationToken)
                .FirstOrDefaultAsync(storageAccount => storageAccount.Name.Equals(storageAccountName, StringComparison.OrdinalIgnoreCase), cancellationToken);

        /// <inheritdoc/>
        public IAsyncEnumerable<ComputeNode> ListComputeNodesAsync(string poolId, DetailLevel detailLevel = null)
            => batchClient.PoolOperations.ListComputeNodes(poolId, detailLevel: detailLevel).ToAsyncEnumerable();

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudTask> ListTasksAsync(string jobId, DetailLevel detailLevel = null)
            => batchClient.JobOperations.ListTasks(jobId, detailLevel: detailLevel).ToAsyncEnumerable();

        /// <inheritdoc/>
        public async Task DisableBatchPoolAutoScaleAsync(string poolId, CancellationToken cancellationToken)
            => await batchClient.PoolOperations.DisableAutoScaleAsync(poolId, cancellationToken: cancellationToken);

        /// <inheritdoc/>
        public async Task EnableBatchPoolAutoScaleAsync(string poolId, bool preemptable, TimeSpan interval, IAzureProxy.BatchPoolAutoScaleFormulaFactory formulaFactory, CancellationToken cancellationToken)
        {
            var (allocationState, _, _, _, currentLowPriority, _, currentDedicated) = await GetFullAllocationStateAsync(poolId, cancellationToken);

            if (allocationState != AllocationState.Steady)
            {
                throw new InvalidOperationException();
            }

            await batchClient.PoolOperations.EnableAutoScaleAsync(poolId, formulaFactory(preemptable, preemptable ? currentLowPriority ?? 0 : currentDedicated ?? 0), interval, cancellationToken: cancellationToken);
        }
    }
}
