﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
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
        private readonly CachingRetryHandler.CachingRetryHandlerPolicy cachingRetry;
        private readonly CachingRetryHandler.CachingAsyncRetryHandlerPolicy cachingAsyncRetry;
        private readonly CachingRetryHandler.CachingAsyncRetryHandlerPolicy cachingAsyncRetryExceptWhenExists;
        private readonly CachingRetryHandler.CachingAsyncRetryHandlerPolicy cachingAsyncRetryExceptWhenNotFound;

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

            this.azureProxy = azureProxy;
            this.logger = logger;

            var sleepDuration = new Func<int, Exception, TimeSpan?>((attempt, exception) => (exception as BatchException)?.RequestInformation?.RetryAfter);

            this.cachingRetry = cachingRetryHandler.PolicyBuilder.OpinionatedRetryPolicy()
                .WithExceptionBasedWaitWithRetryPolicyOptionsBackup(sleepDuration, backupSkipProvidedIncrements: true).SetOnRetryBehavior(this.logger).AddCaching().SyncBuild();

            this.cachingAsyncRetry = cachingRetryHandler.PolicyBuilder.OpinionatedRetryPolicy()
                .WithExceptionBasedWaitWithRetryPolicyOptionsBackup(sleepDuration, backupSkipProvidedIncrements: true).SetOnRetryBehavior(this.logger).AddCaching().AsyncBuild();

            this.cachingAsyncRetryExceptWhenExists = cachingRetryHandler.PolicyBuilder
                .OpinionatedRetryPolicy(Polly.Policy.Handle<BatchException>(ex => !CreationErrorFoundCodes.Contains(ex.RequestInformation?.BatchError?.Code, StringComparer.OrdinalIgnoreCase)))
                .WithExceptionBasedWaitWithRetryPolicyOptionsBackup(sleepDuration, backupSkipProvidedIncrements: true).SetOnRetryBehavior(this.logger).AddCaching().AsyncBuild();

            this.cachingAsyncRetryExceptWhenNotFound = cachingRetryHandler.PolicyBuilder
                .OpinionatedRetryPolicy(Polly.Policy.Handle<BatchException>(ex => !DeletionErrorFoundCodes.Contains(ex.RequestInformation?.BatchError?.Code, StringComparer.OrdinalIgnoreCase)))
                .WithExceptionBasedWaitWithRetryPolicyOptionsBackup(sleepDuration, backupSkipProvidedIncrements: true).SetOnRetryBehavior(this.logger).AddCaching().AsyncBuild();
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
        public Task CreateAutoPoolModeBatchJobAsync(string jobId, CloudTask cloudTask, PoolInformation poolInformation, CancellationToken cancellationToken) => azureProxy.CreateAutoPoolModeBatchJobAsync(jobId, cloudTask, poolInformation, cancellationToken);

        /// <inheritdoc/>
        public async Task CreateBatchJobAsync(PoolInformation poolInformation, CancellationToken cancellationToken)
        {
            try
            {
                await cachingAsyncRetryExceptWhenExists.ExecuteWithRetryAsync(ct => azureProxy.CreateBatchJobAsync(poolInformation, ct), cancellationToken);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.JobExists.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public async Task AddBatchTaskAsync(string tesTaskId, CloudTask cloudTask, PoolInformation poolInformation, CancellationToken cancellationToken)
        {
            try
            {
                await cachingAsyncRetryExceptWhenExists.ExecuteWithRetryAsync(ct => azureProxy.AddBatchTaskAsync(tesTaskId, cloudTask, poolInformation, ct), cancellationToken);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.TaskExists.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public async Task DeleteBatchJobAsync(PoolInformation poolInformation, CancellationToken cancellationToken)
        {
            try
            {
                await cachingAsyncRetryExceptWhenNotFound.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchJobAsync(poolInformation, ct), cancellationToken);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.JobNotFound.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public async Task DeleteBatchJobAsync(string taskId, CancellationToken cancellationToken)
        {
            try
            {
                await cachingAsyncRetryExceptWhenNotFound.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchJobAsync(taskId, ct), cancellationToken);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.JobNotFound.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        public async Task DeleteBatchTaskAsync(string taskId, PoolInformation poolInformation, CancellationToken cancellationToken)
        {
            try
            {
                await cachingAsyncRetryExceptWhenNotFound.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchTaskAsync(taskId, poolInformation, ct), cancellationToken);
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
            catch (BatchException exc) when (BatchErrorCodeStrings.TaskNotFound.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
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
        public Task<IEnumerable<string>> GetActivePoolIdsAsync(string prefix, TimeSpan minAge, CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.GetActivePoolIdsAsync(prefix, minAge, ct), cancellationToken);

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudPool> GetActivePoolsAsync(string hostName) => cachingRetry.ExecuteWithRetry(() => azureProxy.GetActivePoolsAsync(hostName));


        /// <inheritdoc/>
        public int GetBatchActiveJobCount() => cachingRetry.ExecuteWithRetry(azureProxy.GetBatchActiveJobCount);

        /// <inheritdoc/>
        public IEnumerable<AzureBatchNodeCount> GetBatchActiveNodeCountByVmSize() => cachingRetry.ExecuteWithRetry(azureProxy.GetBatchActiveNodeCountByVmSize);

        /// <inheritdoc/>
        public int GetBatchActivePoolCount() => cachingRetry.ExecuteWithRetry(azureProxy.GetBatchActivePoolCount);

        /// <inheritdoc/>
        public Task<AzureBatchJobAndTaskState> GetBatchJobAndTaskStateAsync(Tes.Models.TesTask tesTask, bool usingAutoPools, CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.GetBatchJobAndTaskStateAsync(tesTask, usingAutoPools, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<string> GetNextBatchJobIdAsync(string tesTaskId, CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.GetNextBatchJobIdAsync(tesTaskId, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<IEnumerable<string>> GetPoolIdsReferencedByJobsAsync(CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(azureProxy.GetPoolIdsReferencedByJobsAsync, cancellationToken);

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
        public Task<IEnumerable<Microsoft.WindowsAzure.Storage.Blob.CloudBlob>> ListBlobsAsync(Uri directoryUri, CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.ListBlobsAsync(directoryUri, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<IEnumerable<string>> ListOldJobsToDeleteAsync(TimeSpan oldestJobAge, CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.ListOldJobsToDeleteAsync(oldestJobAge, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<IEnumerable<string>> ListOrphanedJobsToDeleteAsync(TimeSpan minJobAge, CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.ListOrphanedJobsToDeleteAsync(minJobAge, ct), cancellationToken);

        /// <inheritdoc/>
        public Task UploadBlobAsync(Uri blobAbsoluteUri, string content, CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.UploadBlobAsync(blobAbsoluteUri, content, ct), cancellationToken);

        /// <inheritdoc/>
        public Task UploadBlobFromFileAsync(Uri blobAbsoluteUri, string filePath, CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.UploadBlobFromFileAsync(blobAbsoluteUri, filePath, ct), cancellationToken);

        /// <inheritdoc/>
        public Task<Microsoft.WindowsAzure.Storage.Blob.BlobProperties> GetBlobPropertiesAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.GetBlobPropertiesAsync(blobAbsoluteUri, ct), cancellationToken);

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
                return await cachingAsyncRetryExceptWhenExists.ExecuteWithRetryAsync(ct => azureProxy.CreateBatchPoolAsync(poolInfo, isPreemptable, ct), cancellationToken);
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
            => cachingAsyncRetry.ExecuteWithRetryAndCachingAsync(
                $"{nameof(CachingWithRetriesAzureProxy)}:{poolId}",
                ct => azureProxy.GetFullAllocationStateAsync(poolId, ct),
                DateTimeOffset.Now.Add(BatchPoolService.RunInterval).Subtract(TimeSpan.FromSeconds(1)), cancellationToken);

        /// <inheritdoc/>
        public IAsyncEnumerable<ComputeNode> ListComputeNodesAsync(string poolId, DetailLevel detailLevel) => cachingAsyncRetry.ExecuteWithRetryAsync(() => azureProxy.ListComputeNodesAsync(poolId, detailLevel), cachingRetry);

        /// <inheritdoc/>
        public IAsyncEnumerable<CloudTask> ListTasksAsync(string jobId, DetailLevel detailLevel) => cachingAsyncRetry.ExecuteWithRetryAsync(() => azureProxy.ListTasksAsync(jobId, detailLevel), cachingRetry);

        /// <inheritdoc/>
        public Task DisableBatchPoolAutoScaleAsync(string poolId, CancellationToken cancellationToken) => azureProxy.DisableBatchPoolAutoScaleAsync(poolId, cancellationToken);

        /// <inheritdoc/>
        public Task EnableBatchPoolAutoScaleAsync(string poolId, bool preemptable, TimeSpan interval, IAzureProxy.BatchPoolAutoScaleFormulaFactory formulaFactory, CancellationToken cancellationToken) => azureProxy.EnableBatchPoolAutoScaleAsync(poolId, preemptable, interval, formulaFactory, cancellationToken);
    }
}
