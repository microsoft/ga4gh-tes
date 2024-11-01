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
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using CommonUtilities;
using CommonUtilities.AzureCloud;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Rest;
using TesApi.Web.Extensions;
using TesApi.Web.Management;
using TesApi.Web.Management.Configuration;
using TesApi.Web.Storage;
using static CommonUtilities.RetryHandler;
using BatchProtocol = Microsoft.Azure.Batch.Protocol;
using CloudTask = Microsoft.Azure.Batch.CloudTask;
using OnAllTasksComplete = Microsoft.Azure.Batch.Common.OnAllTasksComplete;

namespace TesApi.Web
{
    /// <summary>
    /// Wrapper for Azure APIs
    /// </summary>
    // TODO: Consider breaking the different sets of Azure APIs into their own classes.
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
        private readonly Func<string, string> createNodeManagedIdentityResourceId;
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

            createNodeManagedIdentityResourceId = name => $"/subscriptions/{batchAccountInformation.SubscriptionId}/resourceGroups/{batchAccountInformation.ResourceGroupName}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/{name}";

            if (string.IsNullOrWhiteSpace(batchAccountOptions.Value.AccountName))
            {
                throw new ArgumentException("The batch account name is missing from the the configuration.", nameof(batchAccountOptions));
            }

            batchRetryPolicyWhenJobNotFound = retryHandler.PolicyBuilder
                .OpinionatedRetryPolicy(Polly.Policy.Handle<BatchException>(ex => BatchErrorCodeStrings.JobNotFound.Equals(ex.RequestInformation.BatchError.Code, StringComparison.OrdinalIgnoreCase)))
                .WithExceptionBasedWaitWithRetryPolicyOptionsBackup((attempt, exception) => (exception as BatchException)?.RequestInformation?.RetryAfter)
                .SetOnRetryBehavior(onRetry: LogRetryErrorOnRetryHandler())
                .AsyncBuild();

            batchRetryPolicyWhenNodeNotReady = retryHandler.PolicyBuilder
                .OpinionatedRetryPolicy(Polly.Policy.Handle<BatchException>(ex => "NodeNotReady".Equals(ex.RequestInformation.BatchError.Code, StringComparison.OrdinalIgnoreCase)))
                .WithExceptionBasedWaitWithRetryPolicyOptionsBackup((attempt, exception) => (exception as BatchException)?.RequestInformation?.RetryAfter)
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

            logger.LogDebug("TES: Creating Batch job {BatchJob}", jobId);
            var job = batchClient.JobOperations.CreateJob(jobId, new() { PoolId = poolId });
            job.OnAllTasksComplete = OnAllTasksComplete.NoAction;
            job.OnTaskFailure = OnTaskFailure.NoAction;

            await job.CommitAsync(cancellationToken: cancellationToken);
            logger.LogDebug("TES: Batch job {BatchJob} committed successfully", jobId);
            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
        }

        /// <inheritdoc/>
        public async Task AddBatchTasksAsync(IEnumerable<CloudTask> cloudTasks, string jobId, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrEmpty(jobId);
            ArgumentNullException.ThrowIfNull(cloudTasks);

            cloudTasks = cloudTasks.ToList();
            var job = await batchRetryPolicyWhenJobNotFound.ExecuteWithRetryAsync(ct =>
                batchClient.JobOperations.GetJobAsync(jobId, cancellationToken: ct),
                cancellationToken);

            await job.AddTaskAsync(cloudTasks, new() { CancellationToken = cancellationToken, MaxDegreeOfParallelism = (int)Math.Ceiling((double)cloudTasks.Count() / Constants.MaxTasksInSingleAddTaskCollectionRequest) + 1 });
        }

        /// <inheritdoc/>
        public async Task DeleteBatchJobAsync(string jobId, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrEmpty(jobId);
            logger.LogDebug("Deleting job {BatchJob}", jobId);
            await batchClient.JobOperations.DeleteJobAsync(jobId, cancellationToken: cancellationToken);
        }

        /// <inheritdoc/>
        public async Task TerminateBatchTaskAsync(string tesTaskId, string jobId, CancellationToken cancellationToken)
        {
            var jobFilter = new ODATADetailLevel
            {
                FilterClause = $"startswith(id,'{tesTaskId}{BatchJobAttemptSeparator}')",
                SelectClause = "id"
            };

            List<CloudTask> batchTasksToTerminate = default;

            try
            {
                batchTasksToTerminate = await batchClient.JobOperations.ListTasks(jobId, jobFilter).ToAsyncEnumerable().ToListAsync(cancellationToken);
            }
            catch (BatchException ex) when (ex.InnerException is BatchProtocol.Models.BatchErrorException bee && "JobNotFound".Equals(bee.Body?.Code, StringComparison.InvariantCultureIgnoreCase))
            {
                logger.LogWarning("Job not found for TES task {TesTask}", tesTaskId);
                return; // Task cannot exist if the job is not found.
            }

            if (batchTasksToTerminate.Count > 1)
            {
                logger.LogWarning("Found more than one active task for TES task {TesTask}", tesTaskId);
            }

            foreach (var task in batchTasksToTerminate)
            {
                logger.LogDebug("Terminating task {BatchTask}", task.Id);
                await batchRetryPolicyWhenNodeNotReady.ExecuteWithRetryAsync(ct => task.TerminateAsync(cancellationToken: ct), cancellationToken);
            }
        }

        /// <inheritdoc/>
        public async Task DeleteBatchTaskAsync(string taskId, string jobId, CancellationToken cancellationToken)
        {
            logger.LogDebug("Deleting task {BatchTask}", taskId);
            await batchRetryPolicyWhenNodeNotReady.ExecuteWithRetryAsync(ct => batchClient.JobOperations.DeleteTaskAsync(jobId, taskId, cancellationToken: ct), cancellationToken);
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudPool> GetActivePoolsAsync(string hostName)
        {
            var activePoolsFilter = new ODATADetailLevel
            {
                FilterClause = "state eq 'active'",
                SelectClause = BatchPool.CloudPoolSelectClause + ",identity"
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
                logger.LogError(ex, @"An exception occurred when getting the storage account key for account {StorageAccountName}.", storageAccountInfo.Name);
                throw;
            }
        }

        /// <inheritdoc/>
        public Task UploadBlobAsync(Uri blobAbsoluteUri, string content, CancellationToken cancellationToken)
            => new BlobClient(blobAbsoluteUri, new(BlobClientOptions.ServiceVersion.V2021_04_10))
                .UploadAsync(BinaryData.FromString(content), overwrite: true, cancellationToken);

        /// <inheritdoc/>
        public Task UploadBlobFromFileAsync(Uri blobAbsoluteUri, string filePath, CancellationToken cancellationToken)
            => new BlobClient(blobAbsoluteUri, new(BlobClientOptions.ServiceVersion.V2021_04_10))
                .UploadAsync(filePath, overwrite: true, cancellationToken);

        /// <inheritdoc/>
        public async Task<string> DownloadBlobAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken)
            => (await new BlobClient(blobAbsoluteUri, new(BlobClientOptions.ServiceVersion.V2021_04_10))
                .DownloadContentAsync(cancellationToken)).Value.Content.ToString();

        /// <inheritdoc/>
        public async Task<bool> BlobExistsAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken)
            => await new BlobClient(blobAbsoluteUri, new(BlobClientOptions.ServiceVersion.V2021_04_10))
                .ExistsAsync(cancellationToken);

        /// <inheritdoc/>
        public async Task<BlobProperties> GetBlobPropertiesAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken)
        {
            var blob = new BlobClient(blobAbsoluteUri, new(BlobClientOptions.ServiceVersion.V2021_04_10));

            if (await blob.ExistsAsync(cancellationToken))
            {
                return await blob.GetPropertiesAsync(cancellationToken: cancellationToken);
            }

            return default;
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<BlobNameAndUri> ListBlobsAsync(Uri directoryUri, CancellationToken cancellationToken)
        {
            var directory = new BlobClient(directoryUri, new(BlobClientOptions.ServiceVersion.V2021_04_10));
            return directory.GetParentBlobContainerClient()
                .GetBlobsAsync(prefix: directory.Name.TrimEnd('/') + "/", cancellationToken: cancellationToken)
                .Select(blobItem => new BlobNameAndUri(blobItem.Name, new BlobUriBuilder(directory.Uri) { Sas = null, BlobName = blobItem.Name }.ToUri()));
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<BlobItem> ListBlobsWithTagsAsync(Uri containerUri, string prefix, CancellationToken cancellationToken)
        {
            BlobContainerClient container = new(containerUri, new(BlobClientOptions.ServiceVersion.V2021_04_10));

            return container.GetBlobsAsync(BlobTraits.Tags, prefix: prefix, cancellationToken: cancellationToken);
        }

        /// <inheritdoc/>
        public async Task SetBlobTags(Uri blobAbsoluteUri, IDictionary<string, string> tags, CancellationToken cancellationToken)
        {
            BlobClient blob = new(blobAbsoluteUri, new(BlobClientOptions.ServiceVersion.V2021_04_10));
            using var result = await blob.SetTagsAsync(tags, cancellationToken: cancellationToken);

            if (result.IsError)
            {
                // throw something here.
            }
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
        public Task DisableBatchPoolAutoScaleAsync(string poolId, CancellationToken cancellationToken)
            => batchClient.PoolOperations.DisableAutoScaleAsync(poolId, cancellationToken: cancellationToken);

        /// <inheritdoc/>
        public async Task EnableBatchPoolAutoScaleAsync(string poolId, bool preemptable, TimeSpan interval, IAzureProxy.BatchPoolAutoScaleFormulaFactory formulaFactory, Func<int, ValueTask<int>> currentTargetFunc, CancellationToken cancellationToken)
        {
            var (allocationState, _, _, _, currentLowPriority, _, currentDedicated) = await GetFullAllocationStateAsync(poolId, cancellationToken);

            if (allocationState != AllocationState.Steady)
            {
                throw new InvalidOperationException();
            }

            var formula = formulaFactory(preemptable, await currentTargetFunc(preemptable ? currentLowPriority ?? 0 : currentDedicated ?? 0));
            logger.LogDebug("Setting Pool {PoolID} to AutoScale({AutoScaleInterval}): '{AutoScaleFormula}'", poolId, interval, formula.Replace(Environment.NewLine, @"\n"));
            await batchClient.PoolOperations.EnableAutoScaleAsync(poolId, formula, interval, cancellationToken: cancellationToken);
        }

        /// <inheritdoc/>
        public string GetManagedIdentityInBatchAccountResourceGroup(string identityName)
        {
            return createNodeManagedIdentityResourceId(identityName);
        }
    }
}
