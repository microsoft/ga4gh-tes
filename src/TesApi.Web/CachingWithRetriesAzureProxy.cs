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
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using Tes.ApiClients;
using Tes.ApiClients.Options;
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
        private readonly AsyncRetryPolicy batchPoolOrJobCreateOrTaskAddHandler;

        /// <summary>
        /// Contructor to create a cache of <see cref="IAzureProxy"/>
        /// </summary>
        /// <param name="azureProxy"><see cref="IAzureProxy"/></param>
        /// <param name="retryPolicyOptions"></param>
        /// <param name="cachingRetryHandler"></param>
        /// <param name="logger"></param>
        public CachingWithRetriesAzureProxy(IAzureProxy azureProxy, IOptions<RetryPolicyOptions> retryPolicyOptions, CachingRetryHandler cachingRetryHandler, ILogger<CachingWithRetriesAzureProxy> logger)
        {
            ArgumentNullException.ThrowIfNull(azureProxy);
            ArgumentNullException.ThrowIfNull(cachingRetryHandler);

            this.cachingRetryHandler = cachingRetryHandler;
            this.azureProxy = azureProxy;
            this.logger = logger;

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
        public async Task CreateBatchJobAsync(string jobId, CancellationToken cancellationToken)
        {
            try
            {
                await batchPoolOrJobCreateOrTaskAddHandler.ExecuteAsync(ct => azureProxy.CreateBatchJobAsync(jobId, ct), cancellationToken);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.JobExists.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public async Task AddBatchTaskAsync(string tesTaskId, CloudTask cloudTask, string jobId, CancellationToken cancellationToken)
        {
            try
            {
                await batchPoolOrJobCreateOrTaskAddHandler.ExecuteAsync(ct => azureProxy.AddBatchTaskAsync(tesTaskId, cloudTask, jobId, ct), cancellationToken);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.TaskExists.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public Task DeleteBatchJobAsync(string jobId, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying DeleteBatchJobAsync ({RetryCount}).", retryCount));
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchJobAsync(jobId, ct), cancellationToken, ctx);
        }

        /// <inheritdoc/>
        public Task DeleteBatchTaskAsync(string tesTaskId, string jobId, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying DeleteBatchTaskAsync ({RetryCount}).", retryCount));
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchTaskAsync(tesTaskId, jobId, ct), cancellationToken, ctx);
        }

        /// <inheritdoc/>
        public Task TerminateBatchTaskAsync(string tesTaskId, string jobId, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying TerminateBatchTaskAsync ({RetryCount}).", retryCount));
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.TerminateBatchTaskAsync(tesTaskId, jobId, ct), cancellationToken, ctx);
        }

        /// <inheritdoc/>
        public Task DeleteBatchPoolAsync(string poolId, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying DeleteBatchPoolAsync ({RetryCount}).", retryCount));
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchPoolAsync(poolId, ct), cancellationToken, ctx);
        }

        /// <inheritdoc/>
        public Task<CloudPool> GetBatchPoolAsync(string poolId, CancellationToken cancellationToken, DetailLevel detailLevel)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying GetBatchPoolAsync ({RetryCount}).", retryCount));
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.GetBatchPoolAsync(poolId, ct, detailLevel), cancellationToken, ctx);
        }

        /// <inheritdoc/>
        public Task<CloudJob> GetBatchJobAsync(string jobId, CancellationToken cancellationToken, DetailLevel detailLevel)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying GetBatchJobAsync ({RetryCount}).", retryCount));
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.GetBatchJobAsync(jobId, ct, detailLevel), cancellationToken, ctx);
        }

        /// <inheritdoc/>
        public async Task DeleteBatchComputeNodesAsync(string poolId, IEnumerable<ComputeNode> computeNodes, CancellationToken cancellationToken)
        {
            cachingRetryHandler.AppCache.Remove($"{nameof(CachingWithRetriesAzureProxy)}:{poolId}");
            var ctx = new Context();
            ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying DeleteBatchComputeNodesAsync ({RetryCount}).", retryCount));
            await cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchComputeNodesAsync(poolId, computeNodes, ct), cancellationToken, ctx);
        }

        /// <inheritdoc/>
        public Task<string> DownloadBlobAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying DownloadBlobAsync ({RetryCount}).", retryCount));
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.DownloadBlobAsync(blobAbsoluteUri, ct), cancellationToken, ctx);
        }

        /// <inheritdoc/>
        public Task<bool> BlobExistsAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying BlobExistsAsync ({RetryCount}).", retryCount));
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.BlobExistsAsync(blobAbsoluteUri, ct), cancellationToken, ctx);
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudPool> GetActivePoolsAsync(string hostName)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying GetActivePoolsAsync ({RetryCount}).", retryCount));
            return cachingRetryHandler.ExecuteWithRetry(() => azureProxy.GetActivePoolsAsync(hostName), ctx);
        }


        /// <inheritdoc/>
        public int GetBatchActiveJobCount()
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying GetBatchActiveJobCount ({RetryCount}).", retryCount));
            return cachingRetryHandler.ExecuteWithRetry(azureProxy.GetBatchActiveJobCount, ctx);
        }

        /// <inheritdoc/>
        public IEnumerable<AzureBatchNodeCount> GetBatchActiveNodeCountByVmSize()
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying GetBatchActiveNodeCountByVmSize ({RetryCount}).", retryCount));
            return cachingRetryHandler.ExecuteWithRetry(azureProxy.GetBatchActiveNodeCountByVmSize, ctx);
        }

        /// <inheritdoc/>
        public int GetBatchActivePoolCount()
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying GetBatchActivePoolCount ({RetryCount}).", retryCount));
            return cachingRetryHandler.ExecuteWithRetry(azureProxy.GetBatchActivePoolCount, ctx);
        }

        /// <inheritdoc/>
        public Task<string> GetStorageAccountKeyAsync(StorageAccountInfo storageAccountInfo, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying GetStorageAccountKeyAsync ({RetryCount}).", retryCount));
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
                ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying GetStorageAccountInfoAsync ({RetryCount}).", retryCount));
                storageAccountInfo = await cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.GetStorageAccountInfoAsync(storageAccountName, ct), cancellationToken, ctx);

                if (storageAccountInfo is not null)
                {
                    cachingRetryHandler.AppCache.Set($"{nameof(CachingWithRetriesAzureProxy)}:{storageAccountName}", storageAccountInfo, DateTimeOffset.MaxValue);
                }
            }

            return storageAccountInfo;
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<(string Name, Uri Uri)> ListBlobsAsync(Uri directoryUri, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying ListBlobsAsync ({RetryCount}).", retryCount));
            return cachingRetryHandler.AsyncRetryPolicy.ExecuteAsync(() => azureProxy.ListBlobsAsync(directoryUri, cancellationToken), cachingRetryHandler.RetryPolicy, ctx);
        }

        /// <inheritdoc/>
        public Task UploadBlobAsync(Uri blobAbsoluteUri, string content, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying UploadBlobAsync ({RetryCount}).", retryCount));
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.UploadBlobAsync(blobAbsoluteUri, content, ct), cancellationToken);
        }

        /// <inheritdoc/>
        public Task UploadBlobFromFileAsync(Uri blobAbsoluteUri, string filePath, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying UploadBlobFromFileAsync ({RetryCount}).", retryCount));
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.UploadBlobFromFileAsync(blobAbsoluteUri, filePath, ct), cancellationToken, ctx);
        }

        /// <inheritdoc/>
        public Task<Microsoft.WindowsAzure.Storage.Blob.BlobProperties> GetBlobPropertiesAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying GetBlobPropertiesAsync ({RetryCount}).", retryCount));
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
                return await batchPoolOrJobCreateOrTaskAddHandler.ExecuteAsync(ct => azureProxy.CreateBatchPoolAsync(poolInfo, isPreemptable, ct), cancellationToken);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.PoolExists.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            {
                return await GetBatchPoolAsync(poolInfo.Name, cancellationToken, new ODATADetailLevel { SelectClause = BatchPool.CloudPoolSelectClause });
            }
        }

        /// <inheritdoc/>
        public Task<(AllocationState? AllocationState, bool? AutoScaleEnabled, int? TargetLowPriority, int? CurrentLowPriority, int? TargetDedicated, int? CurrentDedicated)> GetFullAllocationStateAsync(string poolId, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying GetFullAllocationStateAsync ({RetryCount}).", retryCount));
            return cachingRetryHandler.ExecuteWithRetryAndCachingAsync(
                $"{nameof(CachingWithRetriesAzureProxy)}:{poolId}",
                ct => azureProxy.GetFullAllocationStateAsync(poolId, ct),
                DateTimeOffset.Now.Add(BatchPoolService.RunInterval).Subtract(TimeSpan.FromSeconds(1)),
                cancellationToken,
                ctx);
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<ComputeNode> ListComputeNodesAsync(string poolId, DetailLevel detailLevel)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying ListComputeNodesAsync ({RetryCount}).", retryCount));
            return cachingRetryHandler.AsyncRetryPolicy.ExecuteAsync(() => azureProxy.ListComputeNodesAsync(poolId, detailLevel), cachingRetryHandler.RetryPolicy, ctx);
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudTask> ListTasksAsync(string jobId, DetailLevel detailLevel)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying ListTasksAsync ({RetryCount}).", retryCount));
            return cachingRetryHandler.AsyncRetryPolicy.ExecuteAsync(() => azureProxy.ListTasksAsync(jobId, detailLevel), cachingRetryHandler.RetryPolicy, ctx);
        }

        /// <inheritdoc/>
        public Task DisableBatchPoolAutoScaleAsync(string poolId, CancellationToken cancellationToken) => azureProxy.DisableBatchPoolAutoScaleAsync(poolId, cancellationToken);

        /// <inheritdoc/>
        public Task EnableBatchPoolAutoScaleAsync(string poolId, bool preemptable, TimeSpan interval, IAzureProxy.BatchPoolAutoScaleFormulaFactory formulaFactory, Func<int, ValueTask<int>> currentTargetFunc, CancellationToken cancellationToken)
            => azureProxy.EnableBatchPoolAutoScaleAsync(poolId, preemptable, interval, formulaFactory, currentTargetFunc, cancellationToken);

        /// <inheritdoc/>
        public Task<AutoScaleRun> EvaluateAutoScaleAsync(string poolId, string autoscaleFormula, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying EvaluateAutoScaleAsync ({RetryCount}).", retryCount));
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.EvaluateAutoScaleAsync(poolId, autoscaleFormula, ct), cancellationToken, ctx);
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<Azure.Storage.Blobs.Models.BlobItem> ListBlobsWithTagsAsync(Uri containerUri, string prefix, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying ListBlobsWithTagsAsync ({RetryCount}).", retryCount));
            return cachingRetryHandler.AsyncRetryPolicy.ExecuteAsync(() => azureProxy.ListBlobsWithTagsAsync(containerUri, prefix, cancellationToken), cachingRetryHandler.RetryPolicy, ctx);
        }

        /// <inheritdoc/>
        public Task SetBlobTags(Uri blobAbsoluteUri, IDictionary<string, string> tags, CancellationToken cancellationToken)
        {
            var ctx = new Context();
            ctx.SetOnRetryHandler((outcome, timespan, retryCount) => logger.LogError(outcome, "Retrying SetBlobTags ({RetryCount}).", retryCount));
            return cachingRetryHandler.ExecuteWithRetryAsync(ct => azureProxy.SetBlobTags(blobAbsoluteUri, tags, ct), cancellationToken, ctx);
        }
    }
}
