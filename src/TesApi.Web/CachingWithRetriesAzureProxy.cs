// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using LazyCache;
using Microsoft.Azure.Batch;
using Polly;
using Polly.Retry;
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
        private readonly IAppCache cache;

        private static readonly int RetryCount = 3;
        private static TimeSpan SleepDurationProvider(int attempt)
            => TimeSpan.FromSeconds(Math.Pow(2, attempt));

        internal static readonly AsyncRetryPolicy asyncRetryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(RetryCount, SleepDurationProvider);

        internal static readonly RetryPolicy retryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetry(RetryCount, SleepDurationProvider);

        /// <summary>
        /// Contructor to create a cache of <see cref="IAzureProxy"/>
        /// </summary>
        /// <param name="azureProxy"><see cref="IAzureProxy"/></param>
        /// <param name="cache">Lazy cache using <see cref="IAppCache"/></param>
        public CachingWithRetriesAzureProxy(IAzureProxy azureProxy, IAppCache cache)
        {
            this.azureProxy = azureProxy;
            this.cache = cache;
        }


        /// <inheritdoc/>
        public Task CreateAutoPoolModeBatchJobAsync(string jobId, CloudTask cloudTask, PoolInformation poolInformation, CancellationToken cancellationToken) => azureProxy.CreateAutoPoolModeBatchJobAsync(jobId, cloudTask, poolInformation, cancellationToken);

        /// <inheritdoc/>
        public Task CreateBatchJobAsync(PoolInformation poolInformation, CancellationToken cancellationToken) => asyncRetryPolicy.ExecuteAsync(ct => azureProxy.CreateBatchJobAsync(poolInformation, ct), cancellationToken);

        /// <inheritdoc/>
        public Task AddBatchTaskAsync(string tesTaskId, CloudTask cloudTask, PoolInformation poolInformation, CancellationToken cancellationToken) => asyncRetryPolicy.ExecuteAsync(ct => azureProxy.AddBatchTaskAsync(tesTaskId, cloudTask, poolInformation, ct), cancellationToken);

        /// <inheritdoc/>
        public Task DeleteBatchJobAsync(PoolInformation poolInformation, CancellationToken cancellationToken) => asyncRetryPolicy.ExecuteAsync(ct => azureProxy.DeleteBatchJobAsync(poolInformation, ct), cancellationToken);

        /// <inheritdoc/>
        public Task DeleteBatchJobAsync(string taskId, CancellationToken cancellationToken) => asyncRetryPolicy.ExecuteAsync(ct => azureProxy.DeleteBatchJobAsync(taskId, ct), cancellationToken);

        /// <inheritdoc/>
        public Task DeleteBatchTaskAsync(string taskId, PoolInformation poolInformation, CancellationToken cancellationToken) => asyncRetryPolicy.ExecuteAsync(ct => azureProxy.DeleteBatchTaskAsync(taskId, poolInformation, ct), cancellationToken);

        /// <inheritdoc/>
        public Task DeleteBatchPoolAsync(string poolId, CancellationToken cancellationToken) => asyncRetryPolicy.ExecuteAsync(ct => azureProxy.DeleteBatchPoolAsync(poolId, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<CloudPool> GetBatchPoolAsync(string poolId, DetailLevel detailLevel, CancellationToken cancellationToken) => asyncRetryPolicy.ExecuteAsync(ct => azureProxy.GetBatchPoolAsync(poolId, detailLevel, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<CloudJob> GetBatchJobAsync(string jobId, DetailLevel detailLevel, CancellationToken cancellationToken) => asyncRetryPolicy.ExecuteAsync(ct => azureProxy.GetBatchJobAsync(jobId, detailLevel, ct), cancellationToken);

        /// <inheritdoc/>
        public Task DeleteBatchComputeNodesAsync(string poolId, IEnumerable<ComputeNode> computeNodes, CancellationToken cancellationToken)
        {
            cache.Remove(poolId);
            return asyncRetryPolicy.ExecuteAsync(ct => azureProxy.DeleteBatchComputeNodesAsync(poolId, computeNodes, ct), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<string> DownloadBlobAsync(Uri blobAbsoluteUri) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.DownloadBlobAsync(blobAbsoluteUri));

        /// <inheritdoc/>
        public Task<bool> BlobExistsAsync(Uri blobAbsoluteUri) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.BlobExistsAsync(blobAbsoluteUri));

        /// <inheritdoc/>
        public Task<IEnumerable<string>> GetActivePoolIdsAsync(string prefix, TimeSpan minAge, CancellationToken cancellationToken) => asyncRetryPolicy.ExecuteAsync(ct => azureProxy.GetActivePoolIdsAsync(prefix, minAge, ct), cancellationToken);

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudPool> GetActivePoolsAsync(string hostName) => retryPolicy.Execute(() => azureProxy.GetActivePoolsAsync(hostName));


        /// <inheritdoc/>
        public int GetBatchActiveJobCount() => retryPolicy.Execute(() => azureProxy.GetBatchActiveJobCount());

        /// <inheritdoc/>
        public IEnumerable<AzureBatchNodeCount> GetBatchActiveNodeCountByVmSize() => retryPolicy.Execute(() => azureProxy.GetBatchActiveNodeCountByVmSize());

        /// <inheritdoc/>
        public int GetBatchActivePoolCount() => retryPolicy.Execute(() => azureProxy.GetBatchActivePoolCount());

        /// <inheritdoc/>
        public Task<(IReadOnlyDictionary<CloudPool, IReadOnlyList<ComputeNode>> PoolsAndNodes, IReadOnlyDictionary<CloudJob, IReadOnlyList<CloudTask>> JobsAndTasks)> GetBatchAccountStateAsync(bool usingAutoPools, IEnumerable<string> ids, CancellationToken cancellationToken)
            => asyncRetryPolicy.ExecuteAsync(ct => azureProxy.GetBatchAccountStateAsync(usingAutoPools, ids, ct), cancellationToken);

        /// <inheritdoc/>
        public AzureBatchJobAndTaskState GetBatchJobAndTaskState(Tes.Models.TesTask tesTask, bool usingAutoPools, (IReadOnlyDictionary<CloudPool, IReadOnlyList<ComputeNode>> PoolsAndNodes, IReadOnlyDictionary<CloudJob, IReadOnlyList<CloudTask>> JobsAndTasks) batchAccountState)
            => azureProxy.GetBatchJobAndTaskState(tesTask, usingAutoPools, batchAccountState);

        /// <inheritdoc/>
        public Task<string> GetNextBatchJobIdAsync(string tesTaskId) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.GetNextBatchJobIdAsync(tesTaskId));

        /// <inheritdoc/>
        public Task<IEnumerable<string>> GetPoolIdsReferencedByJobsAsync(CancellationToken cancellationToken) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.GetPoolIdsReferencedByJobsAsync(cancellationToken));

        /// <inheritdoc/>
        public Task<string> GetStorageAccountKeyAsync(StorageAccountInfo storageAccountInfo)
            => cache.GetOrAddAsync(storageAccountInfo.Id, () =>
                asyncRetryPolicy.ExecuteAsync(() => azureProxy.GetStorageAccountKeyAsync(storageAccountInfo)), DateTimeOffset.Now.AddHours(1));

        /// <inheritdoc/>
        public async Task<StorageAccountInfo> GetStorageAccountInfoAsync(string storageAccountName)
        {
            var storageAccountInfo = cache.Get<StorageAccountInfo>(storageAccountName);

            if (storageAccountInfo is null)
            {
                storageAccountInfo = await asyncRetryPolicy.ExecuteAsync(() => azureProxy.GetStorageAccountInfoAsync(storageAccountName));

                if (storageAccountInfo is not null)
                {
                    cache.Add(storageAccountName, storageAccountInfo, DateTimeOffset.MaxValue);
                }
            }

            return storageAccountInfo;
        }

        /// <inheritdoc/>
        public Task<IEnumerable<string>> ListBlobsAsync(Uri directoryUri) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.ListBlobsAsync(directoryUri));

        /// <inheritdoc/>
        public Task<IEnumerable<string>> ListOldJobsToDeleteAsync(TimeSpan oldestJobAge) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.ListOldJobsToDeleteAsync(oldestJobAge));

        /// <inheritdoc/>
        public Task<IEnumerable<string>> ListOrphanedJobsToDeleteAsync(TimeSpan minJobAge, CancellationToken cancellationToken) => asyncRetryPolicy.ExecuteAsync(ct => azureProxy.ListOrphanedJobsToDeleteAsync(minJobAge, ct), cancellationToken);

        /// <inheritdoc/>
        public Task UploadBlobAsync(Uri blobAbsoluteUri, string content) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.UploadBlobAsync(blobAbsoluteUri, content));

        /// <inheritdoc/>
        public Task UploadBlobFromFileAsync(Uri blobAbsoluteUri, string filePath) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.UploadBlobFromFileAsync(blobAbsoluteUri, filePath));

        /// <inheritdoc/>
        public bool LocalFileExists(string path) => azureProxy.LocalFileExists(path);

        /// <inheritdoc/>
        public bool TryReadCwlFile(string workflowId, out string content) => azureProxy.TryReadCwlFile(workflowId, out content);

        /// <inheritdoc/>
        public string GetArmRegion() => azureProxy.GetArmRegion();

        /// <inheritdoc/>
        public Task<PoolInformation> CreateBatchPoolAsync(BatchModels.Pool poolInfo, bool isPreemptable) => azureProxy.CreateBatchPoolAsync(poolInfo, isPreemptable);

        /// <inheritdoc/>
        public Task DeleteBatchPoolIfExistsAsync(string poolId, CancellationToken cancellationToken)
            => azureProxy.DeleteBatchPoolIfExistsAsync(poolId, cancellationToken);

        /// <inheritdoc/>
        public async Task<(Microsoft.Azure.Batch.Common.AllocationState? AllocationState, bool? AutoScaleEnabled, int? TargetLowPriority, int? CurrentLowPriority, int? TargetDedicated, int? CurrentDedicated)> GetFullAllocationStateAsync(string poolId, CancellationToken cancellationToken)
        {
            var allocationState = cache.Get<(Microsoft.Azure.Batch.Common.AllocationState? AllocationState, bool? AutoScaleEnabled, int? TargetLowPriority, int? CurrentLowPriority, int? TargetDedicated, int? CurrentDedicated)>(poolId);

            if (default == allocationState)
            {
                allocationState = await asyncRetryPolicy.ExecuteAsync(ct => azureProxy.GetFullAllocationStateAsync(poolId, ct), cancellationToken);

                if (default != allocationState)
                {
                    cache.Add(poolId, allocationState, DateTimeOffset.Now.Add(BatchPoolService.RunInterval).Subtract(TimeSpan.FromSeconds(1)));
                }
            }

            return allocationState;
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<ComputeNode> ListComputeNodesAsync(string poolId, DetailLevel detailLevel) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.ListComputeNodesAsync(poolId, detailLevel), retryPolicy);

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudTask> ListTasksAsync(string jobId, DetailLevel detailLevel) => asyncRetryPolicy.ExecuteAsync(() => azureProxy.ListTasksAsync(jobId, detailLevel), retryPolicy);

        /// <inheritdoc/>
        public Task DisableBatchPoolAutoScaleAsync(string poolId, CancellationToken cancellationToken) => azureProxy.DisableBatchPoolAutoScaleAsync(poolId, cancellationToken);

        /// <inheritdoc/>
        public Task EnableBatchPoolAutoScaleAsync(string poolId, bool preemptable, TimeSpan interval, IAzureProxy.BatchPoolAutoScaleFormulaFactory formulaFactory, CancellationToken cancellationToken) => azureProxy.EnableBatchPoolAutoScaleAsync(poolId, preemptable, interval, formulaFactory, cancellationToken);
    }
}
