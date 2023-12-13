// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using Tes.ApiClients;
using Tes.ApiClients.Options;
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
        private readonly CachingRetryHandler cachingRetryHandler;
        private readonly AsyncRetryPolicy batchPoolOrJobCreateOrTaskAddHandler;

        /// <summary>
        /// Contructor to create a cache of <see cref="IAzureProxy"/>
        /// </summary>
        /// <param name="azureProxy"><see cref="IAzureProxy"/></param>
        /// <param name="retryPolicyOptions"></param>
        /// <param name="cachingRetryHandler"></param>
        public CachingWithRetriesAzureProxy(IAzureProxy azureProxy, IOptions<RetryPolicyOptions> retryPolicyOptions, CachingRetryHandler cachingRetryHandler)
        {
            ArgumentNullException.ThrowIfNull(azureProxy);
            ArgumentNullException.ThrowIfNull(cachingRetryHandler);

            this.cachingRetryHandler = cachingRetryHandler;
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
        public Task DeleteBatchJobAsync(PoolInformation poolInformation, CancellationToken cancellationToken) => cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchJobAsync(poolInformation, ct), cancellationToken);

        /// <inheritdoc/>
        public Task DeleteBatchJobAsync(string taskId, CancellationToken cancellationToken) => cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchJobAsync(taskId, ct), cancellationToken);

        /// <inheritdoc/>
        public Task DeleteBatchTaskAsync(string taskId, PoolInformation poolInformation, CancellationToken cancellationToken) => cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchTaskAsync(taskId, poolInformation, ct), cancellationToken);

        /// <inheritdoc/>
        public Task DeleteBatchPoolAsync(string poolId, CancellationToken cancellationToken) => cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchPoolAsync(poolId, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<CloudPool> GetBatchPoolAsync(string poolId, CancellationToken cancellationToken, DetailLevel detailLevel) => cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.GetBatchPoolAsync(poolId, ct, detailLevel), cancellationToken);

        /// <inheritdoc/>
        public Task<CloudJob> GetBatchJobAsync(string jobId, CancellationToken cancellationToken, DetailLevel detailLevel) => cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.GetBatchJobAsync(jobId, ct, detailLevel), cancellationToken);

        /// <inheritdoc/>
        public async Task DeleteBatchComputeNodesAsync(string poolId, IEnumerable<ComputeNode> computeNodes, CancellationToken cancellationToken)
        {
            cachingRetryHandler.AppCache.Remove($"{nameof(CachingWithRetriesAzureProxy)}:{poolId}");
            await cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchComputeNodesAsync(poolId, computeNodes, ct), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<string> DownloadBlobAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken) => cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DownloadBlobAsync(blobAbsoluteUri, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<bool> BlobExistsAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken) => cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.BlobExistsAsync(blobAbsoluteUri, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<IEnumerable<string>> GetActivePoolIdsAsync(string prefix, TimeSpan minAge, CancellationToken cancellationToken) => cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.GetActivePoolIdsAsync(prefix, minAge, ct), cancellationToken);

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudPool> GetActivePoolsAsync(string hostName) => cachingRetryHandler.ExecuteWithRetry(() => azureProxy.GetActivePoolsAsync(hostName));


        /// <inheritdoc/>
        public int GetBatchActiveJobCount() => cachingRetryHandler.ExecuteWithRetry(() => azureProxy.GetBatchActiveJobCount());

        /// <inheritdoc/>
        public IEnumerable<AzureBatchNodeCount> GetBatchActiveNodeCountByVmSize() => cachingRetryHandler.ExecuteWithRetry(() => azureProxy.GetBatchActiveNodeCountByVmSize());

        /// <inheritdoc/>
        public int GetBatchActivePoolCount() => cachingRetryHandler.ExecuteWithRetry(() => azureProxy.GetBatchActivePoolCount());

        /// <inheritdoc/>
        public Task<AzureBatchJobAndTaskState> GetBatchJobAndTaskStateAsync(Tes.Models.TesTask tesTask, bool usingAutoPools, CancellationToken cancellationToken) => cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.GetBatchJobAndTaskStateAsync(tesTask, usingAutoPools, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<string> GetNextBatchJobIdAsync(string tesTaskId, CancellationToken cancellationToken) => cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.GetNextBatchJobIdAsync(tesTaskId, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<IEnumerable<string>> GetPoolIdsReferencedByJobsAsync(CancellationToken cancellationToken) => cachingRetryHandler.ExecuteWithRetryAsync(azureProxy.GetPoolIdsReferencedByJobsAsync, cancellationToken);

        /// <inheritdoc/>
        public Task<string> GetStorageAccountKeyAsync(StorageAccountInfo storageAccountInfo, CancellationToken cancellationToken)
            => cachingRetryHandler.ExecuteWithRetryAndCachingAsync($"{nameof(CachingWithRetriesAzureProxy)}:{storageAccountInfo.Id}",
                ct => azureProxy.GetStorageAccountKeyAsync(storageAccountInfo, ct), DateTimeOffset.Now.AddHours(1), cancellationToken);

        /// <inheritdoc/>
        public async Task<StorageAccountInfo> GetStorageAccountInfoAsync(string storageAccountName, CancellationToken cancellationToken)
        {
            var storageAccountInfo = cachingRetryHandler.AppCache.Get<StorageAccountInfo>($"{nameof(CachingWithRetriesAzureProxy)}:{storageAccountName}");

            if (storageAccountInfo is null)
            {
                storageAccountInfo = await cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.GetStorageAccountInfoAsync(storageAccountName, ct), cancellationToken);

                if (storageAccountInfo is not null)
                {
                    cachingRetryHandler.AppCache.Set($"{nameof(CachingWithRetriesAzureProxy)}:{storageAccountName}", storageAccountInfo, DateTimeOffset.MaxValue);
                }
            }

            return storageAccountInfo;
        }

        /// <inheritdoc/>
        public Task<IEnumerable<Microsoft.WindowsAzure.Storage.Blob.CloudBlob>> ListBlobsAsync(Uri directoryUri, CancellationToken cancellationToken) => cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.ListBlobsAsync(directoryUri, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<IEnumerable<string>> ListOldJobsToDeleteAsync(TimeSpan oldestJobAge, CancellationToken cancellationToken) => cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.ListOldJobsToDeleteAsync(oldestJobAge, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<IEnumerable<string>> ListOrphanedJobsToDeleteAsync(TimeSpan minJobAge, CancellationToken cancellationToken) => cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.ListOrphanedJobsToDeleteAsync(minJobAge, ct), cancellationToken);

        /// <inheritdoc/>
        public Task UploadBlobAsync(Uri blobAbsoluteUri, string content, CancellationToken cancellationToken) => cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.UploadBlobAsync(blobAbsoluteUri, content, ct), cancellationToken);

        /// <inheritdoc/>
        public Task UploadBlobFromFileAsync(Uri blobAbsoluteUri, string filePath, CancellationToken cancellationToken) => cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.UploadBlobFromFileAsync(blobAbsoluteUri, filePath, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<Microsoft.WindowsAzure.Storage.Blob.BlobProperties> GetBlobPropertiesAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken) => cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.GetBlobPropertiesAsync(blobAbsoluteUri, ct), cancellationToken);

        /// <inheritdoc/>
        public bool LocalFileExists(string path) => azureProxy.LocalFileExists(path);

        /// <inheritdoc/>
        public bool TryReadCwlFile(string workflowId, out string content) => azureProxy.TryReadCwlFile(workflowId, out content);

        /// <inheritdoc/>
        public string GetArmRegion() => azureProxy.GetArmRegion();

        /// <inheritdoc/>
        public async Task<PoolInformation> CreateBatchPoolAsync(BatchModels.Pool poolInfo, bool isPreemptable, CancellationToken cancellationToken)
        {
            try
            {
                return await batchPoolOrJobCreateOrTaskAddHandler.ExecuteAsync(ct => azureProxy.CreateBatchPoolAsync(poolInfo, isPreemptable, ct), cancellationToken);
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
        public Task<FullBatchPoolAllocationState> GetFullAllocationStateAsync(string poolId, CancellationToken cancellationToken)
            => cachingRetryHandler.ExecuteWithRetryAndCachingAsync(
                $"{nameof(CachingWithRetriesAzureProxy)}:{poolId}",
                ct => azureProxy.GetFullAllocationStateAsync(poolId, ct),
                DateTimeOffset.Now.Add(BatchPoolService.RunInterval).Subtract(TimeSpan.FromSeconds(1)), cancellationToken);

        /// <inheritdoc/>
        public IAsyncEnumerable<ComputeNode> ListComputeNodesAsync(string poolId, DetailLevel detailLevel) => cachingRetryHandler.AsyncRetryPolicy.ExecuteAsync(() => azureProxy.ListComputeNodesAsync(poolId, detailLevel), cachingRetryHandler.RetryPolicy);

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudTask> ListTasksAsync(string jobId, DetailLevel detailLevel) => cachingRetryHandler.AsyncRetryPolicy.ExecuteAsync(() => azureProxy.ListTasksAsync(jobId, detailLevel), cachingRetryHandler.RetryPolicy);

        /// <inheritdoc/>
        public Task DisableBatchPoolAutoScaleAsync(string poolId, CancellationToken cancellationToken) => azureProxy.DisableBatchPoolAutoScaleAsync(poolId, cancellationToken);

        /// <inheritdoc/>
        public Task EnableBatchPoolAutoScaleAsync(string poolId, bool preemptable, TimeSpan interval, IAzureProxy.BatchPoolAutoScaleFormulaFactory formulaFactory, CancellationToken cancellationToken) => azureProxy.EnableBatchPoolAutoScaleAsync(poolId, preemptable, interval, formulaFactory, cancellationToken);
    }
}
