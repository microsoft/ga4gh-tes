// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.Azure.Management.ApplicationInsights.Management;
using Microsoft.Azure.Management.Batch;
using Microsoft.Azure.Management.ContainerRegistry.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Rest;
using Polly;
using Polly.Retry;
using TesApi.Web.Extensions;
using TesApi.Web.Management.Batch;
using TesApi.Web.Management.Configuration;
using TesApi.Web.Storage;
using BatchModels = Microsoft.Azure.Management.Batch.Models;
using CloudTask = Microsoft.Azure.Batch.CloudTask;
using FluentAzure = Microsoft.Azure.Management.Fluent.Azure;
using OnAllTasksComplete = Microsoft.Azure.Batch.Common.OnAllTasksComplete;

namespace TesApi.Web
{
    /// <summary>
    /// Wrapper for Azure APIs
    /// </summary>
    public partial class AzureProxy : IAzureProxy
    {
        private const char BatchJobAttemptSeparator = '-';
        private static readonly AsyncRetryPolicy batchRaceConditionJobNotFoundRetryPolicy = Policy
            .Handle<BatchException>(ex => ex.RequestInformation.BatchError.Code == BatchErrorCodeStrings.JobNotFound)
            .WaitAndRetryAsync(5, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)));

        private readonly AsyncRetryPolicy batchNodeNotReadyRetryPolicy;

        private readonly ILogger logger;
        private readonly BatchClient batchClient;
        //private readonly string subscriptionId;
        private readonly string location;
        //private readonly string batchResourceGroupName;
        private readonly string batchAccountName;
        //TODO: This dependency should be injected at a higher level (e.g. scheduler), but that requires significant refactoring that should be done separately.
        private readonly IBatchPoolManager batchPoolManager;


        /// <summary>
        /// Constructor of AzureProxy
        /// </summary>
        /// <param name="batchAccountOptions">The Azure Batch Account options</param>
        /// <param name="batchPoolManager"><inheritdoc cref="IBatchPoolManager"/></param>
        /// <param name="logger">The logger</param>
        /// <exception cref="InvalidOperationException"></exception>
        public AzureProxy(IOptions<BatchAccountOptions> batchAccountOptions, IBatchPoolManager batchPoolManager, ILogger<AzureProxy> logger/*, Azure.Core.TokenCredential tokenCredential*/)
        {
            ArgumentNullException.ThrowIfNull(batchAccountOptions);
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(batchPoolManager);

            this.batchPoolManager = batchPoolManager;

            if (string.IsNullOrWhiteSpace(batchAccountOptions.Value.AccountName))
            {
                //TODO: check if there's a better exception for this scenario or we need to create a custom one.
                throw new InvalidOperationException("The batch account name is missing from the the configuration.");
            }

            this.logger = logger;

            this.batchNodeNotReadyRetryPolicy = Policy
               .Handle<BatchException>(ex => "NodeNotReady".Equals(ex.RequestInformation?.BatchError?.Code, StringComparison.InvariantCultureIgnoreCase))
               .WaitAndRetryAsync(
                    5,
                    (retryAttempt, exception, _) => (exception as BatchException)?.RequestInformation?.RetryAfter ?? TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                    (exception, delay, retryAttempt, _) =>
                        {
                            var requestId = (exception as BatchException)?.RequestInformation?.ServiceRequestId;
                            var reason = (exception.InnerException as Microsoft.Azure.Batch.Protocol.Models.BatchErrorException)?.Response?.ReasonPhrase;
                            this.logger.LogDebug(exception, "Retry attempt {RetryAttempt} after delay {DelaySeconds} for NodeNotReady exception: ServiceRequestId: {ServiceRequestId}, BatchErrorCode: NodeNotReady, Reason: {ReasonPhrase}", retryAttempt, delay.TotalSeconds, requestId, reason);
                            return Task.FromResult(false);
                        });

            if (!string.IsNullOrWhiteSpace(batchAccountOptions.Value.AppKey))
            {
                //If the key is provided assume we won't use ARM and the information will be provided via config
                batchClient = BatchClient.Open(new BatchSharedKeyCredentials(batchAccountOptions.Value.BaseUrl,
                    batchAccountOptions.Value.AccountName, batchAccountOptions.Value.AppKey));
                location = batchAccountOptions.Value.Region;
                //subscriptionId = batchAccountOptions.Value.SubscriptionId;
                //batchResourceGroupName = batchAccountOptions.Value.ResourceGroup;

            }
            else
            {
                batchAccountName = batchAccountOptions.Value.AccountName;
                var (SubscriptionId, ResourceGroupName, Location, BatchAccountEndpoint) = FindBatchAccountAsync(batchAccountName, CancellationToken.None).Result;
                //batchResourceGroupName = ResourceGroupName;
                //subscriptionId = SubscriptionId;
                location = Location;
                batchClient = BatchClient.Open(new BatchTokenCredentials($"https://{BatchAccountEndpoint}", () => GetAzureAccessTokenAsync(CancellationToken.None, "https://batch.core.windows.net/")));
            }

            //azureOfferDurableId = batchAccountOptions.Value.AzureOfferDurableId;

            //if (!AzureRegionUtils.TryGetBillingRegionName(location, out billingRegionName))
            //{
            //    logger.LogWarning($"Azure ARM location '{location}' does not have a corresponding Azure Billing Region.  Prices from the fallback billing region '{DefaultAzureBillingRegionName}' will be used instead.");
            //    billingRegionName = DefaultAzureBillingRegionName;
            //}
        }

        internal AzureProxy() { } // TODO: Remove. Temporary WIP

        // TODO: Static method because the instrumentation key is needed in both Program.cs and Startup.cs and we wanted to avoid intializing the batch client twice.
        // Can we skip initializing app insights with a instrumentation key in Program.cs? If yes, change this to an instance method.
        /// <summary>
        /// Gets the Application Insights instrumentation key
        /// </summary>
        /// <param name="appInsightsApplicationId">Application Insights application id</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>Application Insights instrumentation key</returns>
        public static async Task<string> GetAppInsightsConnectionStringAsync(string appInsightsApplicationId, CancellationToken cancellationToken)
        {
            var azureClient = await GetAzureManagementClientAsync(cancellationToken);
            var subscriptionIds = (await azureClient.Subscriptions.ListAsync(cancellationToken: cancellationToken)).ToAsyncEnumerable().Select(s => s.SubscriptionId);

            var credentials = new TokenCredentials(await GetAzureAccessTokenAsync(cancellationToken));

            await foreach (var subscriptionId in subscriptionIds.WithCancellation(cancellationToken))
            {
                try
                {
                    var components = new ApplicationInsightsManagementClient(credentials) { SubscriptionId = subscriptionId }.Components;
                    var app = await (await components.ListAsync(cancellationToken))
                        .ToAsyncEnumerable(components.ListNextAsync)
                        .FirstOrDefaultAsync(a => a.ApplicationId.Equals(appInsightsApplicationId, StringComparison.OrdinalIgnoreCase), cancellationToken);

                    if (app is not null)
                    {
                        return app.ConnectionString;
                    }
                }
                catch
                {
                }
            }

            return null;
        }

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
        public async Task CreateBatchJobAsync(string jobId, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrEmpty(jobId);

            logger.LogInformation("TES: Creating Batch job {BatchJob}", jobId);
            var job = batchClient.JobOperations.CreateJob(jobId, new() { PoolId = jobId });
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
            var job = await batchRaceConditionJobNotFoundRetryPolicy.ExecuteAsync(ct =>
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
            catch (BatchException ex) when (ex.InnerException is Microsoft.Azure.Batch.Protocol.Models.BatchErrorException bee && "JobNotFound".Equals(bee.Body?.Code, StringComparison.InvariantCultureIgnoreCase))
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
                logger.LogInformation("Terminating task {BatchTask}", task.Id);
                await batchNodeNotReadyRetryPolicy.ExecuteAsync(ct => task.TerminateAsync(cancellationToken: ct), cancellationToken);
            }
        }

        /// <inheritdoc/>
        public async Task DeleteBatchTaskAsync(string tesTaskId, string jobId, CancellationToken cancellationToken)
        {
            var jobFilter = new ODATADetailLevel
            {
                FilterClause = $"startswith(id,'{tesTaskId}{BatchJobAttemptSeparator}')",
                SelectClause = "id"
            };

            List<CloudTask> batchTasksToDelete = default;

            try
            {
                batchTasksToDelete = await batchClient.JobOperations.ListTasks(jobId, jobFilter).ToAsyncEnumerable().ToListAsync(cancellationToken);
            }
            catch (BatchException ex) when (ex.InnerException is Microsoft.Azure.Batch.Protocol.Models.BatchErrorException bee && "JobNotFound".Equals(bee.Body?.Code, StringComparison.InvariantCultureIgnoreCase))
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
                await batchNodeNotReadyRetryPolicy.ExecuteAsync(ct => task.DeleteAsync(cancellationToken: ct), cancellationToken);
            }
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudPool> GetActivePoolsAsync(string hostName)
        {
            var activePoolsFilter = new ODATADetailLevel
            {
                FilterClause = "state eq 'active'",
                SelectClause = BatchPool.CloudPoolSelectClause + ",identity",
            };

            return batchClient.PoolOperations.ListPools(activePoolsFilter).ToAsyncEnumerable()
                .Where(p => hostName.Equals(p.Metadata?.FirstOrDefault(m => BatchScheduler.PoolHostName.Equals(m.Name, StringComparison.Ordinal))?.Value, StringComparison.OrdinalIgnoreCase));
        }

        /// <inheritdoc/>
        public Task DeleteBatchComputeNodesAsync(string poolId, IEnumerable<ComputeNode> computeNodes, CancellationToken cancellationToken = default)
            => batchClient.PoolOperations.RemoveFromPoolAsync(poolId, computeNodes, deallocationOption: ComputeNodeDeallocationOption.Requeue, cancellationToken: cancellationToken);

        /// <inheritdoc/>
        public Task DeleteBatchPoolAsync(string poolId, CancellationToken cancellationToken = default)
            => batchPoolManager.DeleteBatchPoolAsync(poolId, cancellationToken: cancellationToken);

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

        private static async IAsyncEnumerable<StorageAccountInfo> GetAccessibleStorageAccountsAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var azureClient = await GetAzureManagementClientAsync(cancellationToken);

            await foreach (var storageAccountInfo in (await azureClient.Subscriptions.ListAsync(cancellationToken: cancellationToken)).ToAsyncEnumerable()
                .Select(s => s.SubscriptionId).SelectManyAwait(async (subscriptionId, ct) =>
                    (await azureClient.WithSubscription(subscriptionId).StorageAccounts.ListAsync(cancellationToken: cancellationToken)).ToAsyncEnumerable()
                        .Select(a => new StorageAccountInfo { Id = a.Id, Name = a.Name, SubscriptionId = subscriptionId, BlobEndpoint = a.EndPoints.Primary.Blob })))
            {
                yield return storageAccountInfo;
            }
        }

        /// <inheritdoc/>
        public async Task<string> GetStorageAccountKeyAsync(StorageAccountInfo storageAccountInfo, CancellationToken cancellationToken)
        {
            try
            {
                var azureClient = await GetAzureManagementClientAsync(cancellationToken);
                var storageAccount = await azureClient.WithSubscription(storageAccountInfo.SubscriptionId).StorageAccounts.GetByIdAsync(storageAccountInfo.Id, cancellationToken);

                return (await storageAccount.GetKeysAsync(cancellationToken))[0].Value;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, $"An exception occurred when getting the storage account key for account {storageAccountInfo.Name}.");
                throw;
            }
        }

        /// <inheritdoc/>
        public Task UploadBlobAsync(Uri blobAbsoluteUri, string content, CancellationToken cancellationToken)
            => new BlobClient(blobAbsoluteUri, new(BlobClientOptions.ServiceVersion.V2021_04_10))
                .UploadAsync(BinaryData.FromString(content), options: null, cancellationToken);

        /// <inheritdoc/>
        public Task UploadBlobFromFileAsync(Uri blobAbsoluteUri, string filePath, CancellationToken cancellationToken)
            => new BlobClient(blobAbsoluteUri, new(BlobClientOptions.ServiceVersion.V2021_04_10))
                .UploadAsync(filePath, options: null, cancellationToken);

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
                .Select(blobItem => new BlobNameAndUri(blobItem.Name, new BlobUriBuilder(directory.Uri) { Sas = null, BlobName = blobItem.Name, Query = null }.ToUri()));
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

        /// <inheritdoc/>
        public bool LocalFileExists(string path)
            => File.Exists(path);

        /// <inheritdoc/>
        public bool TryReadCwlFile(string workflowId, out string content)
        {
            var fileName = $"cwl_temp_file_{workflowId}.cwl";

            try
            {
                var filePath = Directory.GetFiles("/cromwell-tmp", fileName, SearchOption.AllDirectories).FirstOrDefault();

                if (filePath is not null)
                {
                    content = File.ReadAllText(filePath);
                    return true;
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, $"Error looking up or retrieving contents of CWL file '{fileName}'");
            }

            content = null;
            return false;
        }

        /// <inheritdoc />
        public string GetArmRegion()
            => location;

        private static Task<string> GetAzureAccessTokenAsync(CancellationToken cancellationToken, string resource = "https://management.azure.com/")
            => new AzureServiceTokenProvider().GetAccessTokenAsync(resource, cancellationToken: cancellationToken);

        /// <summary>
        /// Gets an authenticated Azure Client instance
        /// </summary>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>An authenticated Azure Client instance</returns>
        private static async Task<FluentAzure.IAuthenticated> GetAzureManagementClientAsync(CancellationToken cancellationToken)
        {
            var accessToken = await GetAzureAccessTokenAsync(cancellationToken);
            var azureCredentials = new AzureCredentials(new TokenCredentials(accessToken), null, null, AzureEnvironment.AzureGlobalCloud);
            var azureClient = FluentAzure.Authenticate(azureCredentials);

            return azureClient;
        }

        /// <inheritdoc/>
        public async Task<CloudPool> CreateBatchPoolAsync(BatchModels.Pool poolInfo, bool isPreemptable, CancellationToken cancellationToken)
        {
            logger.LogInformation("Creating batch pool named {PoolName} with vmSize {PoolVmSize} and low priority {IsPreemptable}", poolInfo.Name, poolInfo.VmSize, isPreemptable);
            var poolId = await batchPoolManager.CreateBatchPoolAsync(poolInfo, isPreemptable, cancellationToken);
            logger.LogInformation("Successfully created batch pool named {PoolName} with vmSize {PoolVmSize} and low priority {IsPreemptable}", poolInfo.Name, poolInfo.VmSize, isPreemptable);

            return await batchClient.PoolOperations.GetPoolAsync(poolId, detailLevel: new ODATADetailLevel { SelectClause = BatchPool.CloudPoolSelectClause }, cancellationToken: cancellationToken);
        }

        // https://learn.microsoft.com/azure/azure-resource-manager/management/move-resource-group-and-subscription#changed-resource-id
        [GeneratedRegex("/*/resourceGroups/([^/]*)/*")]
        private static partial Regex GetResourceGroupRegex();

        private static async Task<(string SubscriptionId, string ResourceGroupName, string Location, string BatchAccountEndpoint)> FindBatchAccountAsync(string batchAccountName, CancellationToken cancellationToken)
        {
            var resourceGroupRegex = GetResourceGroupRegex();
            var tokenCredentials = new TokenCredentials(await GetAzureAccessTokenAsync(cancellationToken));
            var azureClient = await GetAzureManagementClientAsync(cancellationToken);

            var subscriptionIds = (await azureClient.Subscriptions.ListAsync(cancellationToken: cancellationToken)).ToAsyncEnumerable().Select(s => s.SubscriptionId);

            await foreach (var subId in subscriptionIds.WithCancellation(cancellationToken))
            {
                var batchAccountOperations = new BatchManagementClient(tokenCredentials) { SubscriptionId = subId }.BatchAccount;
                var batchAccount = await (await batchAccountOperations.ListAsync(cancellationToken))
                    .ToAsyncEnumerable(batchAccountOperations.ListNextAsync)
                    .FirstOrDefaultAsync(a => a.Name.Equals(batchAccountName, StringComparison.OrdinalIgnoreCase), cancellationToken);

                if (batchAccount is not null)
                {
                    var resourceGroupName = resourceGroupRegex.Match(batchAccount.Id).Groups[1].Value;

                    return (subId, resourceGroupName, batchAccount.Location, batchAccount.AccountEndpoint);
                }
            }

            throw new Exception($"Batch account '{batchAccountName}' does not exist or the TES app service does not have Contributor role on the account.");
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
        public Task<AutoScaleRun> EvaluateAutoScaleAsync(string poolId, string autoscaleFormula, CancellationToken cancellationToken)
            => batchClient.PoolOperations.EvaluateAutoScaleAsync(poolId, autoscaleFormula, cancellationToken: cancellationToken);
    }
}
