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

        /// <summary>
        /// A logging Polly retry handler.
        /// </summary>
        /// <param name="caller">Calling method name.</param>
        /// <returns><see cref="RetryHandler.OnRetryHandler"/></returns>
        private RetryHandler.OnRetryHandler LogRetryErrorOnRetryHandler([System.Runtime.CompilerServices.CallerMemberName] string caller = default)
            => new((exception, timeSpan, retryCount, correlationId) =>
            {
                logger?.LogError(exception, @"Retrying in {Method}: RetryCount: {RetryCount} RetryCount: {TimeSpan} CorrelationId: {CorrelationId:D}", caller, retryCount, timeSpan, correlationId);
            });


        /// <inheritdoc/>
        public async Task CreateBatchJobAsync(string jobId, CancellationToken cancellationToken)
        {
            try
            {
                var ctx = new Context();
                ctx.SetOnRetryHandler(OnRetryMicrosoftAzureBatchCommonBatchExceptionExceptWhenExists(LogRetryErrorOnRetryHandler()));
                await cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.CreateBatchJobAsync(jobId, ct), cancellationToken, ctx);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.JobExists.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public async Task AddBatchTaskAsync(string tesTaskId, CloudTask cloudTask, string jobId, CancellationToken cancellationToken)
        {
            try
            {
                var ctx = new Context();
                ctx.SetOnRetryHandler(OnRetryMicrosoftAzureBatchCommonBatchExceptionExceptWhenExists(LogRetryErrorOnRetryHandler()));
                await cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.AddBatchTaskAsync(tesTaskId, cloudTask, jobId, ct), cancellationToken, ctx);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.TaskExists.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public async Task DeleteBatchJobAsync(string jobId, CancellationToken cancellationToken)
        {
            try
            {
                var ctx = new Context();
                ctx.SetOnRetryHandler(OnRetryMicrosoftAzureBatchCommonBatchExceptionExceptWhenNotFound(LogRetryErrorOnRetryHandler()));
                await cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchJobAsync(jobId, ct), cancellationToken, ctx);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.JobNotFound.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public async Task DeleteBatchTaskAsync(string cloudTaskId, string jobId, CancellationToken cancellationToken)
        {
            try
            {
                var ctx = new Context();
                ctx.SetOnRetryHandler(OnRetryMicrosoftAzureBatchCommonBatchExceptionExceptWhenNotFound(LogRetryErrorOnRetryHandler()));
                await cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchTaskAsync(cloudTaskId, jobId, ct), cancellationToken, ctx);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.TaskNotFound.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public Task TerminateBatchTaskAsync(string tesTaskId, string jobId, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler(LogRetryErrorOnRetryHandler());
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.TerminateBatchTaskAsync(tesTaskId, jobId, ct), cancellationToken, ctx);
        }

        /// <inheritdoc/>
        public async Task DeleteBatchPoolAsync(string poolId, CancellationToken cancellationToken)
        {
            try
            {
                var ctx = new Context();
                ctx.SetOnRetryHandler(OnRetryMicrosoftAzureBatchCommonBatchExceptionExceptWhenNotFound(LogRetryErrorOnRetryHandler()));
                await cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchPoolAsync(poolId, ct), cancellationToken, ctx);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.PoolNotFound.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public Task<CloudPool> GetBatchPoolAsync(string poolId, CancellationToken cancellationToken, DetailLevel detailLevel)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler(LogRetryErrorOnRetryHandler());
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.GetBatchPoolAsync(poolId, ct, detailLevel), cancellationToken, ctx);
        }

        /// <inheritdoc/>
        public Task<CloudJob> GetBatchJobAsync(string jobId, CancellationToken cancellationToken, DetailLevel detailLevel)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler(LogRetryErrorOnRetryHandler());
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.GetBatchJobAsync(jobId, ct, detailLevel), cancellationToken, ctx);
        }

        /// <inheritdoc/>
        public async Task DeleteBatchComputeNodesAsync(string poolId, IEnumerable<ComputeNode> computeNodes, CancellationToken cancellationToken)
        {
            cachingRetryHandler.AppCache.Remove($"{nameof(CachingWithRetriesAzureProxy)}:{poolId}");
            var ctx = new Context();
            ctx.SetOnRetryHandler(LogRetryErrorOnRetryHandler());
            await cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchComputeNodesAsync(poolId, computeNodes, ct), cancellationToken, ctx);
        }

        /// <inheritdoc/>
        public Task<string> DownloadBlobAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler(LogRetryErrorOnRetryHandler());
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DownloadBlobAsync(blobAbsoluteUri, ct), cancellationToken, ctx);
        }

        /// <inheritdoc/>
        public Task<bool> BlobExistsAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler(LogRetryErrorOnRetryHandler());
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.BlobExistsAsync(blobAbsoluteUri, ct), cancellationToken, ctx);
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudPool> GetActivePoolsAsync(string hostName)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler(LogRetryErrorOnRetryHandler());
            return cachingRetryHandler.ExecuteWithRetry(() => azureProxy.GetActivePoolsAsync(hostName), ctx);
        }


        /// <inheritdoc/>
        public int GetBatchActiveJobCount()
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler(LogRetryErrorOnRetryHandler());
            return cachingRetryHandler.ExecuteWithRetry(azureProxy.GetBatchActiveJobCount, ctx);
        }

        /// <inheritdoc/>
        public IEnumerable<AzureBatchNodeCount> GetBatchActiveNodeCountByVmSize()
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler(LogRetryErrorOnRetryHandler());
            return cachingRetryHandler.ExecuteWithRetry(azureProxy.GetBatchActiveNodeCountByVmSize, ctx);
        }

        /// <inheritdoc/>
        public int GetBatchActivePoolCount()
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler(LogRetryErrorOnRetryHandler());
            return cachingRetryHandler.ExecuteWithRetry(azureProxy.GetBatchActivePoolCount, ctx);
        }

        /// <inheritdoc/>
        public Task<string> GetStorageAccountKeyAsync(StorageAccountInfo storageAccountInfo, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler(LogRetryErrorOnRetryHandler());
            return cachingRetryHandler.ExecuteWithRetryAndCachingAsync($"{nameof(CachingWithRetriesAzureProxy)}:{storageAccountInfo.Id}",
                ct => azureProxy.GetStorageAccountKeyAsync(storageAccountInfo, ct), DateTimeOffset.Now.AddHours(1), cancellationToken, ctx);
        }

        /// <inheritdoc/>
        public async Task<StorageAccountInfo> GetStorageAccountInfoAsync(string storageAccountName, CancellationToken cancellationToken)
        {
            var storageAccountInfo = cachingRetryHandler.AppCache.Get<StorageAccountInfo>($"{nameof(CachingWithRetriesAzureProxy)}:{storageAccountName}");

            if (storageAccountInfo is null)
            {
                var ctx = new Context();
                ctx.SetOnRetryHandler(LogRetryErrorOnRetryHandler());
                storageAccountInfo = await cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.GetStorageAccountInfoAsync(storageAccountName, ct), cancellationToken, ctx);

                if (storageAccountInfo is not null)
                {
                    cachingRetryHandler.AppCache.Set($"{nameof(CachingWithRetriesAzureProxy)}:{storageAccountName}", storageAccountInfo, DateTimeOffset.MaxValue);
                }
            }

            return storageAccountInfo;
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<BlobNameAndUri> ListBlobsAsync(Uri directoryUri, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler(LogRetryErrorOnRetryHandler());
            return cachingRetryHandler.AsyncRetryPolicy.ExecuteAsync(() => azureProxy.ListBlobsAsync(directoryUri, cancellationToken), cachingRetryHandler.RetryPolicy, ctx);
        }

        /// <inheritdoc/>
        public Task UploadBlobAsync(Uri blobAbsoluteUri, string content, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler(LogRetryErrorOnRetryHandler());
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.UploadBlobAsync(blobAbsoluteUri, content, ct), cancellationToken);
        }

        /// <inheritdoc/>
        public Task UploadBlobFromFileAsync(Uri blobAbsoluteUri, string filePath, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler(LogRetryErrorOnRetryHandler());
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.UploadBlobFromFileAsync(blobAbsoluteUri, filePath, ct), cancellationToken, ctx);
        }

        /// <inheritdoc/>
        public Task<Azure.Storage.Blobs.Models.BlobProperties> GetBlobPropertiesAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler(LogRetryErrorOnRetryHandler());
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.GetBlobPropertiesAsync(blobAbsoluteUri, ct), cancellationToken, ctx);
        }

        /// <inheritdoc/>
        public bool LocalFileExists(string path) => azureProxy.LocalFileExists(path);

        /// <inheritdoc/>
        public bool TryReadCwlFile(string workflowId, out string content) => azureProxy.TryReadCwlFile(workflowId, out content);

        /// <inheritdoc/>
        public string GetArmRegion() => azureProxy.GetArmRegion();

        /// <inheritdoc/>
        public async Task<CloudPool> CreateBatchPoolAsync(BatchModels.Pool poolInfo, bool isPreemptable, CancellationToken cancellationToken)
        {
            try
            {
                var ctx = new Context();
                ctx.SetOnRetryHandler(OnRetryMicrosoftAzureBatchCommonBatchExceptionExceptWhenExists(LogRetryErrorOnRetryHandler()));
                return await cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.CreateBatchPoolAsync(poolInfo, isPreemptable, ct), cancellationToken, ctx);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.PoolExists.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            {
                return await GetBatchPoolAsync(poolInfo.Name, cancellationToken, new ODATADetailLevel { SelectClause = BatchPool.CloudPoolSelectClause });
            }
        }

        /// <inheritdoc/>
        public Task<FullBatchPoolAllocationState> GetFullAllocationStateAsync(string poolId, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler(LogRetryErrorOnRetryHandler());
            return cachingRetryHandler.ExecuteWithRetryAndCachingAsync(
                $"{nameof(CachingWithRetriesAzureProxy)}:{poolId}",
                ct => azureProxy.GetFullAllocationStateAsync(poolId, ct),
                DateTimeOffset.UtcNow.Add(PoolScheduler.RunInterval.Divide(2)),
                cancellationToken,
                ctx);
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<ComputeNode> ListComputeNodesAsync(string poolId, DetailLevel detailLevel)
        {
            try
            {
                var ctx = new Context();
                ctx.SetOnRetryHandler(OnRetryMicrosoftAzureBatchCommonBatchExceptionExceptWhenNotFound(LogRetryErrorOnRetryHandler()));
                return cachingRetryHandler.AsyncRetryPolicy.ExecuteAsync(() => azureProxy.ListComputeNodesAsync(poolId, detailLevel), cachingRetryHandler.RetryPolicy, ctx);
            }
            catch (BatchException exc) when (exc.RequestInformation.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return AsyncEnumerable.Empty<ComputeNode>();
            }
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudTask> ListTasksAsync(string jobId, DetailLevel detailLevel)
        {
            try
            {
                var ctx = new Context();
                ctx.SetOnRetryHandler(OnRetryMicrosoftAzureBatchCommonBatchExceptionExceptWhenNotFound(LogRetryErrorOnRetryHandler()));
                return cachingRetryHandler.AsyncRetryPolicy.ExecuteAsync(() => azureProxy.ListTasksAsync(jobId, detailLevel), cachingRetryHandler.RetryPolicy, ctx);
            }
            catch (BatchException exc) when (exc.RequestInformation.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return AsyncEnumerable.Empty<CloudTask>();
            }
        }

        /// <inheritdoc/>
        public Task DisableBatchPoolAutoScaleAsync(string poolId, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler(LogRetryErrorOnRetryHandler());
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DisableBatchPoolAutoScaleAsync(poolId, ct), cancellationToken, ctx);
        }

        /// <inheritdoc/>
        public Task EnableBatchPoolAutoScaleAsync(string poolId, bool preemptable, TimeSpan interval, IAzureProxy.BatchPoolAutoScaleFormulaFactory formulaFactory, Func<int, ValueTask<int>> currentTargetFunc, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler(LogRetryErrorOnRetryHandler());
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.EnableBatchPoolAutoScaleAsync(poolId, preemptable, interval, formulaFactory, currentTargetFunc, ct), cancellationToken, ctx);
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<Azure.Storage.Blobs.Models.BlobItem> ListBlobsWithTagsAsync(Uri containerUri, string prefix, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler(LogRetryErrorOnRetryHandler());
            return cachingRetryHandler.AsyncRetryPolicy.ExecuteAsync(() => azureProxy.ListBlobsWithTagsAsync(containerUri, prefix, cancellationToken), cachingRetryHandler.RetryPolicy, ctx);
        }

        /// <inheritdoc/>
        public Task SetBlobTags(Uri blobAbsoluteUri, IDictionary<string, string> tags, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler(LogRetryErrorOnRetryHandler());
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.SetBlobTags(blobAbsoluteUri, tags, ct), cancellationToken, ctx);
        }
    }
}
