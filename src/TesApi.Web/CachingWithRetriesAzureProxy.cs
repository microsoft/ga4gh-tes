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
using Microsoft.Extensions.Logging;
using Polly;
using Tes.ApiClients;
using TesApi.Web.Extensions;
using TesApi.Web.Storage;
using BatchModels = Microsoft.Azure.Management.Batch.Models;

namespace TesApi.Web
{
    /// <summary>
    /// Implements caching and retries for <see cref="IAzureProxy"/>.
    /// </summary>
    public class CachingWithRetriesAzureProxy : IAzureProxy
    {
        private readonly ILogger logger;
        private readonly IAzureProxy azureProxy;
        private readonly CachingRetryHandler cachingRetryHandler;

        /// <summary>
        /// Contructor to create a cache of <see cref="IAzureProxy"/>
        /// </summary>
        /// <param name="azureProxy"><see cref="IAzureProxy"/></param>
        /// <param name="cachingRetryHandler"></param>
        /// <param name="logger"></param>
        public CachingWithRetriesAzureProxy(IAzureProxy azureProxy, CachingRetryHandler cachingRetryHandler, ILogger<CachingWithRetriesAzureProxy> logger)
        {
            ArgumentNullException.ThrowIfNull(azureProxy);
            ArgumentNullException.ThrowIfNull(cachingRetryHandler);

            this.cachingRetryHandler = cachingRetryHandler;
            this.azureProxy = azureProxy;
            this.logger = logger;
        }

        private static readonly string[] CreationErrorFoundCodes = new[]
        {
            BatchErrorCodeStrings.TaskExists,
            BatchErrorCodeStrings.PoolExists,
            BatchErrorCodeStrings.JobExists
        };

        private static readonly string[] DeletionErrorFoundCodes = new[]
        {
            BatchErrorCodeStrings.TaskNotFound,
            BatchErrorCodeStrings.PoolNotFound,
            BatchErrorCodeStrings.JobNotFound
        };

        /// <summary>
        /// Rethrows exception if exception is <see cref="BatchException"/> and the Batch API call returned <see cref="System.Net.HttpStatusCode.Conflict"/> otherwise invokes <paramref name="OnRetry"/>.
        /// </summary>
        /// <param name="OnRetry">Polly retry handler.</param>
        /// <returns><see cref="RetryHandler.OnRetryHandler"/></returns>
        private static RetryHandler.OnRetryHandler OnRetryMicrosoftAzureBatchCommonBatchExceptionExceptWhenExists(RetryHandler.OnRetryHandler OnRetry)
            => new((outcome, timespan, retryCount, correlationId) =>
            {
                if (outcome is BatchException batchException && CreationErrorFoundCodes.Contains(batchException.RequestInformation?.BatchError?.Code, StringComparer.OrdinalIgnoreCase))
                {
                    ExceptionDispatchInfo.Capture(outcome).Throw();
                }
                OnRetry?.Invoke(outcome, timespan, retryCount, correlationId);
            });

        /// <summary>
        /// Rethrows exception if exception is <see cref="BatchException"/> and the Batch API call returned <see cref="System.Net.HttpStatusCode.NotFound"/> otherwise invokes <paramref name="OnRetry"/>.
        /// </summary>
        /// <param name="OnRetry">Polly retry handler.</param>
        /// <returns><see cref="RetryHandler.OnRetryHandler"/></returns>
        private static RetryHandler.OnRetryHandler OnRetryMicrosoftAzureBatchCommonBatchExceptionExceptWhenNotFound(RetryHandler.OnRetryHandler OnRetry)
            => new((outcome, timespan, retryCount, correlationId) =>
            {
                if (outcome is BatchException batchException && DeletionErrorFoundCodes.Contains(batchException.RequestInformation?.BatchError?.Code, StringComparer.OrdinalIgnoreCase))
                {
                    ExceptionDispatchInfo.Capture(outcome).Throw();
                }
                OnRetry?.Invoke(outcome, timespan, retryCount, correlationId);
            });


        /// <inheritdoc/>
        public Task CreateAutoPoolModeBatchJobAsync(string jobId, CloudTask cloudTask, PoolInformation poolInformation, CancellationToken cancellationToken) => azureProxy.CreateAutoPoolModeBatchJobAsync(jobId, cloudTask, poolInformation, cancellationToken);

        /// <inheritdoc/>
        public async Task CreateBatchJobAsync(PoolInformation poolInformation, CancellationToken cancellationToken)
        {
            try
            {
                await cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.CreateBatchJobAsync(poolInformation, ct), cancellationToken,
                    OnRetryMicrosoftAzureBatchCommonBatchExceptionExceptWhenExists(RetryHandler.LogRetryErrorOnRetryHandler(logger)));
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.JobExists.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public async Task AddBatchTaskAsync(string tesTaskId, CloudTask cloudTask, PoolInformation poolInformation, CancellationToken cancellationToken)
        {
            try
            {
                await cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.AddBatchTaskAsync(tesTaskId, cloudTask, poolInformation, ct), cancellationToken,
                    OnRetryMicrosoftAzureBatchCommonBatchExceptionExceptWhenExists(RetryHandler.LogRetryErrorOnRetryHandler(logger)));
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.TaskExists.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public async Task DeleteBatchJobAsync(PoolInformation poolInformation, CancellationToken cancellationToken)
        {
            try
            {
                await cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchJobAsync(poolInformation, ct), cancellationToken,
                    OnRetryMicrosoftAzureBatchCommonBatchExceptionExceptWhenNotFound(RetryHandler.LogRetryErrorOnRetryHandler(logger)));
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.JobNotFound.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public async Task DeleteBatchJobAsync(string taskId, CancellationToken cancellationToken)
        {
            try
            {
                await cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchJobAsync(taskId, ct), cancellationToken,
                    OnRetryMicrosoftAzureBatchCommonBatchExceptionExceptWhenNotFound(RetryHandler.LogRetryErrorOnRetryHandler(logger)));
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.JobNotFound.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public async Task DeleteBatchTaskAsync(string taskId, PoolInformation poolInformation, CancellationToken cancellationToken)
        {
            try
            {
                await cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchTaskAsync(taskId, poolInformation, ct), cancellationToken,
                    OnRetryMicrosoftAzureBatchCommonBatchExceptionExceptWhenNotFound(RetryHandler.LogRetryErrorOnRetryHandler(logger)));
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.TaskNotFound.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public async Task DeleteBatchPoolAsync(string poolId, CancellationToken cancellationToken)
        {
            try
            {
                await cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchPoolAsync(poolId, ct), cancellationToken,
                    OnRetryMicrosoftAzureBatchCommonBatchExceptionExceptWhenNotFound(RetryHandler.LogRetryErrorOnRetryHandler(logger)));
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.TaskNotFound.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public Task<CloudPool> GetBatchPoolAsync(string poolId, CancellationToken cancellationToken, DetailLevel detailLevel)
        {
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.GetBatchPoolAsync(poolId, ct, detailLevel), cancellationToken,
                RetryHandler.LogRetryErrorOnRetryHandler(logger));
        }

        /// <inheritdoc/>
        public Task<CloudJob> GetBatchJobAsync(string jobId, CancellationToken cancellationToken, DetailLevel detailLevel)
        {
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.GetBatchJobAsync(jobId, ct, detailLevel), cancellationToken,
                RetryHandler.LogRetryErrorOnRetryHandler(logger));
        }

        /// <inheritdoc/>
        public async Task DeleteBatchComputeNodesAsync(string poolId, IEnumerable<ComputeNode> computeNodes, CancellationToken cancellationToken)
        {
            cachingRetryHandler.AppCache.Remove($"{nameof(CachingWithRetriesAzureProxy)}:{poolId}");
            await cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchComputeNodesAsync(poolId, computeNodes, ct), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<string> DownloadBlobAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken)
        {
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DownloadBlobAsync(blobAbsoluteUri, ct), cancellationToken,
                RetryHandler.LogRetryErrorOnRetryHandler(logger));
        }

        /// <inheritdoc/>
        public Task<bool> BlobExistsAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken)
        {
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.BlobExistsAsync(blobAbsoluteUri, ct), cancellationToken,
                RetryHandler.LogRetryErrorOnRetryHandler(logger));
        }

        /// <inheritdoc/>
        public Task<IEnumerable<string>> GetActivePoolIdsAsync(string prefix, TimeSpan minAge, CancellationToken cancellationToken)
        {
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.GetActivePoolIdsAsync(prefix, minAge, ct), cancellationToken,
                RetryHandler.LogRetryErrorOnRetryHandler(logger));
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudPool> GetActivePoolsAsync(string hostName)
        {
            return cachingRetryHandler.ExecuteWithRetry(() => azureProxy.GetActivePoolsAsync(hostName),
                RetryHandler.LogRetryErrorOnRetryHandler(logger));
        }


        /// <inheritdoc/>
        public int GetBatchActiveJobCount()
        {
            return cachingRetryHandler.ExecuteWithRetry(azureProxy.GetBatchActiveJobCount,
                RetryHandler.LogRetryErrorOnRetryHandler(logger));
        }

        /// <inheritdoc/>
        public IEnumerable<AzureBatchNodeCount> GetBatchActiveNodeCountByVmSize()
        {
            return cachingRetryHandler.ExecuteWithRetry(azureProxy.GetBatchActiveNodeCountByVmSize,
                RetryHandler.LogRetryErrorOnRetryHandler(logger));
        }

        /// <inheritdoc/>
        public int GetBatchActivePoolCount()
        {
            return cachingRetryHandler.ExecuteWithRetry(azureProxy.GetBatchActivePoolCount,
                RetryHandler.LogRetryErrorOnRetryHandler(logger));
        }

        /// <inheritdoc/>
        public Task<AzureBatchJobAndTaskState> GetBatchJobAndTaskStateAsync(Tes.Models.TesTask tesTask, bool usingAutoPools, CancellationToken cancellationToken)
        {
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.GetBatchJobAndTaskStateAsync(tesTask, usingAutoPools, ct), cancellationToken,
                RetryHandler.LogRetryErrorOnRetryHandler(logger));
        }

        /// <inheritdoc/>
        public Task<string> GetNextBatchJobIdAsync(string tesTaskId, CancellationToken cancellationToken)
        {
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.GetNextBatchJobIdAsync(tesTaskId, ct), cancellationToken,
                RetryHandler.LogRetryErrorOnRetryHandler(logger));
        }

        /// <inheritdoc/>
        public Task<IEnumerable<string>> GetPoolIdsReferencedByJobsAsync(CancellationToken cancellationToken)
        {
            return cachingRetryHandler.ExecuteWithRetryAsync(azureProxy.GetPoolIdsReferencedByJobsAsync, cancellationToken,
                RetryHandler.LogRetryErrorOnRetryHandler(logger));
        }

        /// <inheritdoc/>
        public Task<string> GetStorageAccountKeyAsync(StorageAccountInfo storageAccountInfo, CancellationToken cancellationToken)
        {
            return cachingRetryHandler.ExecuteWithRetryAndCachingAsync($"{nameof(CachingWithRetriesAzureProxy)}:{storageAccountInfo.Id}",
                ct => azureProxy.GetStorageAccountKeyAsync(storageAccountInfo, ct), DateTimeOffset.Now.AddHours(1), cancellationToken,
                RetryHandler.LogRetryErrorOnRetryHandler(logger));
        }

        /// <inheritdoc/>
        public async Task<StorageAccountInfo> GetStorageAccountInfoAsync(string storageAccountName, CancellationToken cancellationToken)
        {
            var storageAccountInfo = cachingRetryHandler.AppCache.Get<StorageAccountInfo>($"{nameof(CachingWithRetriesAzureProxy)}:{storageAccountName}");

            if (storageAccountInfo is null)
            {
                storageAccountInfo = await cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.GetStorageAccountInfoAsync(storageAccountName, ct), cancellationToken,
                    RetryHandler.LogRetryErrorOnRetryHandler(logger));

                if (storageAccountInfo is not null)
                {
                    cachingRetryHandler.AppCache.Set($"{nameof(CachingWithRetriesAzureProxy)}:{storageAccountName}", storageAccountInfo, DateTimeOffset.MaxValue);
                }
            }

            return storageAccountInfo;
        }

        /// <inheritdoc/>
        public Task<IEnumerable<Microsoft.WindowsAzure.Storage.Blob.CloudBlob>> ListBlobsAsync(Uri directoryUri, CancellationToken cancellationToken)
        {
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.ListBlobsAsync(directoryUri, ct), cancellationToken,
                RetryHandler.LogRetryErrorOnRetryHandler(logger));
        }

        /// <inheritdoc/>
        public Task<IEnumerable<string>> ListOldJobsToDeleteAsync(TimeSpan oldestJobAge, CancellationToken cancellationToken)
        {
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.ListOldJobsToDeleteAsync(oldestJobAge, ct), cancellationToken,
                RetryHandler.LogRetryErrorOnRetryHandler(logger));
        }

        /// <inheritdoc/>
        public Task<IEnumerable<string>> ListOrphanedJobsToDeleteAsync(TimeSpan minJobAge, CancellationToken cancellationToken)
        {
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.ListOrphanedJobsToDeleteAsync(minJobAge, ct), cancellationToken,
                RetryHandler.LogRetryErrorOnRetryHandler(logger));
        }

        /// <inheritdoc/>
        public Task UploadBlobAsync(Uri blobAbsoluteUri, string content, CancellationToken cancellationToken)
        {
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.UploadBlobAsync(blobAbsoluteUri, content, ct), cancellationToken,
                RetryHandler.LogRetryErrorOnRetryHandler(logger));
        }

        /// <inheritdoc/>
        public Task UploadBlobFromFileAsync(Uri blobAbsoluteUri, string filePath, CancellationToken cancellationToken)
        {
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.UploadBlobFromFileAsync(blobAbsoluteUri, filePath, ct), cancellationToken,
                RetryHandler.LogRetryErrorOnRetryHandler(logger));
        }

        /// <inheritdoc/>
        public Task<Microsoft.WindowsAzure.Storage.Blob.BlobProperties> GetBlobPropertiesAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken)
        {
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.GetBlobPropertiesAsync(blobAbsoluteUri, ct), cancellationToken,
                RetryHandler.LogRetryErrorOnRetryHandler(logger));
        }

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
                return await cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.CreateBatchPoolAsync(poolInfo, isPreemptable, ct), cancellationToken,
                    OnRetryMicrosoftAzureBatchCommonBatchExceptionExceptWhenExists(RetryHandler.LogRetryErrorOnRetryHandler(logger)));
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
        {
            return cachingRetryHandler.ExecuteWithRetryAndCachingAsync(
                $"{nameof(CachingWithRetriesAzureProxy)}:{poolId}",
                ct => azureProxy.GetFullAllocationStateAsync(poolId, ct),
                DateTimeOffset.Now.Add(BatchPoolService.RunInterval).Subtract(TimeSpan.FromSeconds(1)),
                cancellationToken, RetryHandler.LogRetryErrorOnRetryHandler(logger));
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<ComputeNode> ListComputeNodesAsync(string poolId, DetailLevel detailLevel)
        {
            return cachingRetryHandler.AsyncRetryPolicy.ExecuteAsync(() => azureProxy.ListComputeNodesAsync(poolId, detailLevel), cachingRetryHandler.RetryPolicy,
                RetryHandler.LogRetryErrorOnRetryHandler(logger));
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudTask> ListTasksAsync(string jobId, DetailLevel detailLevel)
        {
            return cachingRetryHandler.AsyncRetryPolicy.ExecuteAsync(() => azureProxy.ListTasksAsync(jobId, detailLevel), cachingRetryHandler.RetryPolicy,
                RetryHandler.LogRetryErrorOnRetryHandler(logger));
        }

        /// <inheritdoc/>
        public Task DisableBatchPoolAutoScaleAsync(string poolId, CancellationToken cancellationToken) => azureProxy.DisableBatchPoolAutoScaleAsync(poolId, cancellationToken);

        /// <inheritdoc/>
        public Task EnableBatchPoolAutoScaleAsync(string poolId, bool preemptable, TimeSpan interval, IAzureProxy.BatchPoolAutoScaleFormulaFactory formulaFactory, CancellationToken cancellationToken)
            => azureProxy.EnableBatchPoolAutoScaleAsync(poolId, preemptable, interval, formulaFactory, cancellationToken);
    }
}
