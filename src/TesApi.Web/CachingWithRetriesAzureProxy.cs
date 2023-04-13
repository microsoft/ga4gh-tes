// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

using LazyCache;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;

using TesApi.Web.Management.Configuration;
using TesApi.Web.Storage;

using BatchModels = Microsoft.Azure.Management.Batch.Models;

namespace TesApi.Web
{
    /// <summary>
    /// Implements caching and retries for <see cref="IAzureProxy"/>.
    /// </summary>
    public class CachingWithRetriesAzureProxy : IAzureProxy
    {
        private readonly IAzureProxy azureProxy;
        private readonly Management.CacheAndRetryHandler cacheAndRetryHandler;
        private readonly AsyncRetryPolicy batchPoolOrJobCreateOrTaskAddHandler;

        /// <summary>
        /// Contructor to create a cache of <see cref="IAzureProxy"/>
        /// </summary>
        /// <param name="azureProxy"><see cref="IAzureProxy"/></param>
        /// <param name="retryPolicyOptions"></param>
        /// <param name="cacheAndRetryHandler"></param>
        public CachingWithRetriesAzureProxy(IAzureProxy azureProxy, IOptions<RetryPolicyOptions> retryPolicyOptions, Management.CacheAndRetryHandler cacheAndRetryHandler)
        {
            ArgumentNullException.ThrowIfNull(azureProxy);
            ArgumentNullException.ThrowIfNull(cacheAndRetryHandler);

            this.cacheAndRetryHandler = cacheAndRetryHandler;
            this.azureProxy = azureProxy;

            var creationErrorFoundCodes = new string[]
            {
                BatchErrorCodeStrings.TaskExists,
                BatchErrorCodeStrings.PoolExists,
                BatchErrorCodeStrings.JobExists
            };

            batchPoolOrJobCreateOrTaskAddHandler = Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(retryPolicyOptions.Value.MaxRetryCount,
                    (attempt) => TimeSpan.FromSeconds(Math.Pow(retryPolicyOptions.Value.ExponentialBackOffExponent, attempt)),
                    (exception, timeSpan) =>
                    {
                        if (exception is BatchException batchException && creationErrorFoundCodes.Contains(batchException.RequestInformation?.BatchError?.Code, StringComparer.OrdinalIgnoreCase))
                        {
                            ExceptionDispatchInfo.Capture(exception).Throw();
                        }
                    });
        }


        /// <inheritdoc/>
        public Task CreateAutoPoolModeBatchJobAsync(string jobId, CloudTask cloudTask, PoolInformation poolInformation, CancellationToken cancellationToken) => azureProxy.CreateAutoPoolModeBatchJobAsync(jobId, cloudTask, poolInformation, cancellationToken);

        /// <inheritdoc/>
        public async Task CreateBatchJobAsync(PoolInformation poolInformation, CancellationToken cancellationToken)
        {
            try
            {
                await batchPoolOrJobCreateOrTaskAddHandler.ExecuteAsync(ct => azureProxy.CreateBatchJobAsync(poolInformation, ct), cancellationToken);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.JobExists.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public async Task AddBatchTaskAsync(string tesTaskId, CloudTask cloudTask, PoolInformation poolInformation, CancellationToken cancellationToken)
        {
            try
            {
                await batchPoolOrJobCreateOrTaskAddHandler.ExecuteAsync(ct => azureProxy.AddBatchTaskAsync(tesTaskId, cloudTask, poolInformation, ct), cancellationToken);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.TaskExists.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public Task DeleteBatchJobAsync(PoolInformation poolInformation, CancellationToken cancellationToken) => cacheAndRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchJobAsync(poolInformation, ct), cancellationToken);

        /// <inheritdoc/>
        public Task DeleteBatchJobAsync(string taskId, CancellationToken cancellationToken) => cacheAndRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchJobAsync(taskId, ct), cancellationToken);

        /// <inheritdoc/>
        public Task DeleteBatchTaskAsync(string taskId, PoolInformation poolInformation, CancellationToken cancellationToken) => cacheAndRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchTaskAsync(taskId, poolInformation, ct), cancellationToken);

        /// <inheritdoc/>
        public Task DeleteBatchPoolAsync(string poolId, CancellationToken cancellationToken) => cacheAndRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchPoolAsync(poolId, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<CloudPool> GetBatchPoolAsync(string poolId, DetailLevel detailLevel, CancellationToken cancellationToken) => cacheAndRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.GetBatchPoolAsync(poolId, detailLevel, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<CloudJob> GetBatchJobAsync(string jobId, DetailLevel detailLevel, CancellationToken cancellationToken) => cacheAndRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.GetBatchJobAsync(jobId, detailLevel, ct), cancellationToken);

        /// <inheritdoc/>
        public async Task DeleteBatchComputeNodesAsync(string poolId, IEnumerable<ComputeNode> computeNodes, CancellationToken cancellationToken)
        {
            cacheAndRetryHandler.AppCache.Remove($"{nameof(CachingWithRetriesAzureProxy)}:{poolId}");
            await cacheAndRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchComputeNodesAsync(poolId, computeNodes, ct), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<string> DownloadBlobAsync(Uri blobAbsoluteUri) => cacheAndRetryHandler.ExecuteWithRetryAsync(() => azureProxy.DownloadBlobAsync(blobAbsoluteUri));

        /// <inheritdoc/>
        public Task<bool> BlobExistsAsync(Uri blobAbsoluteUri) => cacheAndRetryHandler.ExecuteWithRetryAsync(() => azureProxy.BlobExistsAsync(blobAbsoluteUri));

        /// <inheritdoc/>
        public Task<IEnumerable<string>> GetActivePoolIdsAsync(string prefix, TimeSpan minAge, CancellationToken cancellationToken) => cacheAndRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.GetActivePoolIdsAsync(prefix, minAge, ct), cancellationToken);

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudPool> GetActivePoolsAsync(string hostName) => cacheAndRetryHandler.ExecuteWithRetry(() => azureProxy.GetActivePoolsAsync(hostName));


        /// <inheritdoc/>
        public int GetBatchActiveJobCount() => cacheAndRetryHandler.ExecuteWithRetry(() => azureProxy.GetBatchActiveJobCount());

        /// <inheritdoc/>
        public IEnumerable<AzureBatchNodeCount> GetBatchActiveNodeCountByVmSize() => cacheAndRetryHandler.ExecuteWithRetry(() => azureProxy.GetBatchActiveNodeCountByVmSize());

        /// <inheritdoc/>
        public int GetBatchActivePoolCount() => cacheAndRetryHandler.ExecuteWithRetry(() => azureProxy.GetBatchActivePoolCount());

        /// <inheritdoc/>
        public Task<AzureBatchJobAndTaskState> GetBatchJobAndTaskStateAsync(Tes.Models.TesTask tesTask, bool usingAutoPools) => cacheAndRetryHandler.ExecuteWithRetryAsync(() => azureProxy.GetBatchJobAndTaskStateAsync(tesTask, usingAutoPools));

        /// <inheritdoc/>
        public Task<string> GetNextBatchJobIdAsync(string tesTaskId) => cacheAndRetryHandler.ExecuteWithRetryAsync(() => azureProxy.GetNextBatchJobIdAsync(tesTaskId));

        /// <inheritdoc/>
        public Task<IEnumerable<string>> GetPoolIdsReferencedByJobsAsync(CancellationToken cancellationToken) => cacheAndRetryHandler.ExecuteWithRetryAsync(() => azureProxy.GetPoolIdsReferencedByJobsAsync(cancellationToken));

        /// <inheritdoc/>
        public Task<string> GetStorageAccountKeyAsync(StorageAccountInfo storageAccountInfo)
            => cacheAndRetryHandler.ExecuteWithRetryAndCachingAsync($"{nameof(CachingWithRetriesAzureProxy)}:{storageAccountInfo.Id}",
                () => azureProxy.GetStorageAccountKeyAsync(storageAccountInfo), DateTimeOffset.Now.AddHours(1));

        /// <inheritdoc/>
        public async Task<StorageAccountInfo> GetStorageAccountInfoAsync(string storageAccountName)
        {
            var storageAccountInfo = cacheAndRetryHandler.AppCache.Get<StorageAccountInfo>($"{nameof(CachingWithRetriesAzureProxy)}:{storageAccountName}");

            if (storageAccountInfo is null)
            {
                storageAccountInfo = await cacheAndRetryHandler.ExecuteWithRetryAsync(() => azureProxy.GetStorageAccountInfoAsync(storageAccountName));

                if (storageAccountInfo is not null)
                {
                    cacheAndRetryHandler.AppCache.Add($"{nameof(CachingWithRetriesAzureProxy)}:{storageAccountName}", storageAccountInfo, DateTimeOffset.MaxValue);
                }
            }

            return storageAccountInfo;
        }

        /// <inheritdoc/>
        public Task<IEnumerable<string>> ListBlobsAsync(Uri directoryUri) => cacheAndRetryHandler.ExecuteWithRetryAsync(() => azureProxy.ListBlobsAsync(directoryUri));

        /// <inheritdoc/>
        public Task<IEnumerable<string>> ListOldJobsToDeleteAsync(TimeSpan oldestJobAge) => cacheAndRetryHandler.ExecuteWithRetryAsync(() => azureProxy.ListOldJobsToDeleteAsync(oldestJobAge));

        /// <inheritdoc/>
        public Task<IEnumerable<string>> ListOrphanedJobsToDeleteAsync(TimeSpan minJobAge, CancellationToken cancellationToken) => cacheAndRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.ListOrphanedJobsToDeleteAsync(minJobAge, ct), cancellationToken);

        /// <inheritdoc/>
        public Task UploadBlobAsync(Uri blobAbsoluteUri, string content) => cacheAndRetryHandler.ExecuteWithRetryAsync(() => azureProxy.UploadBlobAsync(blobAbsoluteUri, content));

        /// <inheritdoc/>
        public Task UploadBlobFromFileAsync(Uri blobAbsoluteUri, string filePath) => cacheAndRetryHandler.ExecuteWithRetryAsync(() => azureProxy.UploadBlobFromFileAsync(blobAbsoluteUri, filePath));

        /// <inheritdoc/>
        public bool LocalFileExists(string path) => azureProxy.LocalFileExists(path);

        /// <inheritdoc/>
        public bool TryReadCwlFile(string workflowId, out string content) => azureProxy.TryReadCwlFile(workflowId, out content);

        /// <inheritdoc/>
        public string GetArmRegion() => azureProxy.GetArmRegion();

        /// <inheritdoc/>
        public async Task<PoolInformation> CreateBatchPoolAsync(BatchModels.Pool poolInfo, bool isPreemptable)
        {
            try
            {
                return await batchPoolOrJobCreateOrTaskAddHandler.ExecuteAsync(() => azureProxy.CreateBatchPoolAsync(poolInfo, isPreemptable));
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.PoolExists.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            {
                return new() { PoolId = poolInfo.Name };
            }
        }

        /// <inheritdoc/>
        public Task DeleteBatchPoolIfExistsAsync(string poolId, CancellationToken cancellationToken)
            => azureProxy.DeleteBatchPoolIfExistsAsync(poolId, cancellationToken);

        /// <inheritdoc/>
        public Task<(AllocationState? AllocationState, bool? AutoScaleEnabled, int? TargetLowPriority, int? CurrentLowPriority, int? TargetDedicated, int? CurrentDedicated)> GetFullAllocationStateAsync(string poolId, CancellationToken cancellationToken)
            => cacheAndRetryHandler.ExecuteWithRetryAndCachingAsync(
                $"{nameof(CachingWithRetriesAzureProxy)}:{poolId}",
                ct => azureProxy.GetFullAllocationStateAsync(poolId, ct),
                DateTimeOffset.Now.Add(BatchPoolService.RunInterval).Subtract(TimeSpan.FromSeconds(1)), cancellationToken);

        /// <inheritdoc/>
        public IAsyncEnumerable<ComputeNode> ListComputeNodesAsync(string poolId, DetailLevel detailLevel) => cacheAndRetryHandler.AsyncRetryPolicy.ExecuteAsync(() => azureProxy.ListComputeNodesAsync(poolId, detailLevel), cacheAndRetryHandler.RetryPolicy);

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudTask> ListTasksAsync(string jobId, DetailLevel detailLevel) => cacheAndRetryHandler.AsyncRetryPolicy.ExecuteAsync(() => azureProxy.ListTasksAsync(jobId, detailLevel), cacheAndRetryHandler.RetryPolicy);

        /// <inheritdoc/>
        public Task DisableBatchPoolAutoScaleAsync(string poolId, CancellationToken cancellationToken) => azureProxy.DisableBatchPoolAutoScaleAsync(poolId, cancellationToken);

        /// <inheritdoc/>
        public Task EnableBatchPoolAutoScaleAsync(string poolId, bool preemptable, TimeSpan interval, IAzureProxy.BatchPoolAutoScaleFormulaFactory formulaFactory, CancellationToken cancellationToken) => azureProxy.EnableBatchPoolAutoScaleAsync(poolId, preemptable, interval, formulaFactory, cancellationToken);
    }
}
