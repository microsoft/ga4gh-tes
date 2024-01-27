﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CommonUtilities;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Tes.ApiClients;
using TesApi.Web.Storage;
using static Tes.ApiClients.CachingRetryHandler;
using BatchModels = Microsoft.Azure.Management.Batch.Models;

namespace TesApi.Web
{
    /// <summary>
    /// Implements caching and retries for <see cref="IAzureProxy"/>.
    /// </summary>
    public class CachingWithRetriesAzureProxy : IAzureProxy
    {
        private readonly IAzureProxy azureProxy;
        private readonly CachingRetryHandlerPolicy cachingRetry;
        private readonly CachingAsyncRetryHandlerPolicy cachingAsyncRetry;
        private readonly CachingAsyncRetryHandlerPolicy cachingAsyncRetryExceptWhenExists;
        private readonly CachingAsyncRetryHandlerPolicy cachingAsyncRetryExceptWhenNotFound;

        /// <summary>
        /// Contructor to create a cache of <see cref="IAzureProxy"/>
        /// </summary>
        /// <param name="azureProxy"><see cref="IAzureProxy"/></param>
        /// <param name="cachingRetryHandler"></param>
        /// <param name="logger"></param>
        public CachingWithRetriesAzureProxy(IAzureProxy azureProxy, CachingRetryPolicyBuilder cachingRetryHandler, ILogger<CachingWithRetriesAzureProxy> logger)
        {
            ArgumentNullException.ThrowIfNull(azureProxy);
            ArgumentNullException.ThrowIfNull(cachingRetryHandler);

            this.azureProxy = azureProxy;
            var sleepDuration = new Func<int, Exception, TimeSpan?>((attempt, exception) => (exception as BatchException)?.RequestInformation?.RetryAfter);

            this.cachingRetry = cachingRetryHandler.PolicyBuilder.OpinionatedRetryPolicy()
                .WithExceptionBasedWaitWithRetryPolicyOptionsBackup(sleepDuration, backupSkipProvidedIncrements: true).SetOnRetryBehavior(logger).AddCaching().SyncBuild();

            this.cachingAsyncRetry = cachingRetryHandler.PolicyBuilder.OpinionatedRetryPolicy()
                .WithExceptionBasedWaitWithRetryPolicyOptionsBackup(sleepDuration, backupSkipProvidedIncrements: true).SetOnRetryBehavior(logger).AddCaching().AsyncBuild();

            this.cachingAsyncRetryExceptWhenExists = cachingRetryHandler.PolicyBuilder
                .OpinionatedRetryPolicy(Polly.Policy.Handle<BatchException>(ex => !CreationErrorFoundCodes.Contains(ex.RequestInformation?.BatchError?.Code, StringComparer.OrdinalIgnoreCase)))
                .WithExceptionBasedWaitWithRetryPolicyOptionsBackup(sleepDuration, backupSkipProvidedIncrements: true).SetOnRetryBehavior(logger).AddCaching().AsyncBuild();

            this.cachingAsyncRetryExceptWhenNotFound = cachingRetryHandler.PolicyBuilder
                .OpinionatedRetryPolicy(Polly.Policy.Handle<BatchException>(ex => !DeletionErrorFoundCodes.Contains(ex.RequestInformation?.BatchError?.Code, StringComparer.OrdinalIgnoreCase)))
                .WithExceptionBasedWaitWithRetryPolicyOptionsBackup(sleepDuration, backupSkipProvidedIncrements: true).SetOnRetryBehavior(logger).AddCaching().AsyncBuild();
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


        /// <inheritdoc/>
        public async Task CreateBatchJobAsync(string jobId, string poolId, CancellationToken cancellationToken)
        {
            try
            {
                await cachingAsyncRetryExceptWhenExists.ExecuteWithRetryAsync(ct => azureProxy.CreateBatchJobAsync(jobId, poolId, ct), cancellationToken);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.JobExists.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public async Task AddBatchTasksAsync(IEnumerable<CloudTask> cloudTasks, string jobId, CancellationToken cancellationToken)
        {
            try
            {
                await cachingAsyncRetryExceptWhenExists.ExecuteWithRetryAsync(ct => azureProxy.AddBatchTasksAsync(cloudTasks, jobId, ct), cancellationToken);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.TaskExists.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public async Task DeleteBatchJobAsync(string jobId, CancellationToken cancellationToken)
        {
            try
            {
                await cachingAsyncRetryExceptWhenNotFound.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchJobAsync(jobId, ct), cancellationToken);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.JobNotFound.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public async Task DeleteBatchTaskAsync(string cloudTaskId, string jobId, CancellationToken cancellationToken)
        {
            try
            {
                await cachingAsyncRetryExceptWhenNotFound.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchTaskAsync(cloudTaskId, jobId, ct), cancellationToken);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.TaskNotFound.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public async Task TerminateBatchTaskAsync(string tesTaskId, string jobId, CancellationToken cancellationToken)
        {
            try
            {
                await cachingAsyncRetryExceptWhenNotFound.ExecuteWithRetryAsync(ct => azureProxy.TerminateBatchTaskAsync(tesTaskId, jobId, ct), cancellationToken);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.TaskNotFound.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public async Task DeleteBatchPoolAsync(string poolId, CancellationToken cancellationToken)
        {
            try
            {
                await cachingAsyncRetryExceptWhenNotFound.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchPoolAsync(poolId, ct), cancellationToken);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.PoolNotFound.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public Task<CloudPool> GetBatchPoolAsync(string poolId, CancellationToken cancellationToken, DetailLevel detailLevel) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.GetBatchPoolAsync(poolId, ct, detailLevel), cancellationToken);

        /// <inheritdoc/>
        public Task<CloudJob> GetBatchJobAsync(string jobId, CancellationToken cancellationToken, DetailLevel detailLevel) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.GetBatchJobAsync(jobId, ct, detailLevel), cancellationToken);

        /// <inheritdoc/>
        public async Task DeleteBatchComputeNodesAsync(string poolId, IEnumerable<ComputeNode> computeNodes, CancellationToken cancellationToken)
        {
            cachingAsyncRetry.AppCache.Remove($"{nameof(CachingWithRetriesAzureProxy)}:{poolId}");
            await cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchComputeNodesAsync(poolId, computeNodes, ct), cancellationToken);
        }

        /// <inheritdoc/>
        public Task<string> DownloadBlobAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.DownloadBlobAsync(blobAbsoluteUri, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<bool> BlobExistsAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.BlobExistsAsync(blobAbsoluteUri, ct), cancellationToken);

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudPool> GetActivePoolsAsync(string hostName) => cachingRetry.ExecuteWithRetry(() => azureProxy.GetActivePoolsAsync(hostName));


        /// <inheritdoc/>
        public int GetBatchActiveJobCount() => cachingRetry.ExecuteWithRetry(azureProxy.GetBatchActiveJobCount);

        /// <inheritdoc/>
        public IEnumerable<AzureBatchNodeCount> GetBatchActiveNodeCountByVmSize() => cachingRetry.ExecuteWithRetry(azureProxy.GetBatchActiveNodeCountByVmSize);

        /// <inheritdoc/>
        public int GetBatchActivePoolCount() => cachingRetry.ExecuteWithRetry(azureProxy.GetBatchActivePoolCount);

        /// <inheritdoc/>
        public Task<string> GetStorageAccountKeyAsync(StorageAccountInfo storageAccountInfo, CancellationToken cancellationToken)
            => cachingAsyncRetry.ExecuteWithRetryAndCachingAsync($"{nameof(CachingWithRetriesAzureProxy)}:{storageAccountInfo.Id}",
                ct => azureProxy.GetStorageAccountKeyAsync(storageAccountInfo, ct), DateTimeOffset.Now.AddHours(1), cancellationToken);

        /// <inheritdoc/>
        public async Task<StorageAccountInfo> GetStorageAccountInfoAsync(string storageAccountName, CancellationToken cancellationToken)
        {
            var cacheKey = $"{nameof(CachingWithRetriesAzureProxy)}:{storageAccountName}";
            var storageAccountInfo = cachingAsyncRetry.AppCache.Get<StorageAccountInfo>(cacheKey);

            if (storageAccountInfo is null)
            {
                storageAccountInfo = await cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.GetStorageAccountInfoAsync(storageAccountName, ct), cancellationToken);

                if (storageAccountInfo is not null)
                {
                    cachingAsyncRetry.AppCache.Set(cacheKey, storageAccountInfo, DateTimeOffset.MaxValue);
                }
            }

            return storageAccountInfo;
        }

        /// <inheritdoc/>
        public Task UploadBlobAsync(Uri blobAbsoluteUri, string content, CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.UploadBlobAsync(blobAbsoluteUri, content, ct), cancellationToken);

        /// <inheritdoc/>
        public Task UploadBlobFromFileAsync(Uri blobAbsoluteUri, string filePath, CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.UploadBlobFromFileAsync(blobAbsoluteUri, filePath, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<Azure.Storage.Blobs.Models.BlobProperties> GetBlobPropertiesAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.GetBlobPropertiesAsync(blobAbsoluteUri, ct), cancellationToken);

        /// <inheritdoc/>
        public string GetArmRegion() => azureProxy.GetArmRegion();

        /// <inheritdoc/>
        public async Task<string> CreateBatchPoolAsync(BatchModels.Pool poolSpec, bool isPreemptable, CancellationToken cancellationToken)
        {
            try
            {
                return await cachingAsyncRetryExceptWhenExists.ExecuteWithRetryAsync(ct => azureProxy.CreateBatchPoolAsync(poolSpec, isPreemptable, ct), cancellationToken);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.PoolExists.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            {
                return poolSpec.Name;
            }
        }

        /// <inheritdoc/>
        public Task<FullBatchPoolAllocationState> GetFullAllocationStateAsync(string poolId, CancellationToken cancellationToken)
            => cachingAsyncRetry.ExecuteWithRetryAndCachingAsync(
                $"{nameof(CachingWithRetriesAzureProxy)}:{poolId}",
                ct => azureProxy.GetFullAllocationStateAsync(poolId, ct),
                DateTimeOffset.UtcNow.Add(PoolScheduler.RunInterval.Divide(2)),
                cancellationToken);

        /// <inheritdoc/>
        public IAsyncEnumerable<ComputeNode> ListComputeNodesAsync(string poolId, DetailLevel detailLevel) => cachingAsyncRetry.ExecuteWithRetryAsync(() => azureProxy.ListComputeNodesAsync(poolId, detailLevel), cachingRetry);

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudTask> ListTasksAsync(string jobId, DetailLevel detailLevel) => cachingAsyncRetry.ExecuteWithRetryAsync(() => azureProxy.ListTasksAsync(jobId, detailLevel), cachingRetry);

        /// <inheritdoc/>
        public Task DisableBatchPoolAutoScaleAsync(string poolId, CancellationToken cancellationToken)
        {
            return cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.DisableBatchPoolAutoScaleAsync(poolId, ct), cancellationToken);
        }

        /// <inheritdoc/>
        public Task EnableBatchPoolAutoScaleAsync(string poolId, bool preemptable, TimeSpan interval, IAzureProxy.BatchPoolAutoScaleFormulaFactory formulaFactory, Func<int, ValueTask<int>> currentTargetFunc, CancellationToken cancellationToken)
        {
            return cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.EnableBatchPoolAutoScaleAsync(poolId, preemptable, interval, formulaFactory, currentTargetFunc, ct), cancellationToken);
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<BlobNameAndUri> ListBlobsAsync(Uri directoryUri, CancellationToken cancellationToken)
        {
            return cachingAsyncRetry.ExecuteWithRetryAsync(() => azureProxy.ListBlobsAsync(directoryUri, cancellationToken), cachingRetry);
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<Azure.Storage.Blobs.Models.BlobItem> ListBlobsWithTagsAsync(Uri containerUri, string prefix, CancellationToken cancellationToken)
        {
            return cachingAsyncRetry.ExecuteWithRetryAsync(() => azureProxy.ListBlobsWithTagsAsync(containerUri, prefix, cancellationToken), cachingRetry);
        }

        /// <inheritdoc/>
        public Task SetBlobTags(Uri blobAbsoluteUri, IDictionary<string, string> tags, CancellationToken cancellationToken)
        {
            return cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.SetBlobTags(blobAbsoluteUri, tags, ct), cancellationToken);
        }
    }
}
