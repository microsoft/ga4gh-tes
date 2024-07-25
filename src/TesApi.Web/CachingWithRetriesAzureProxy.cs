// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using CommonUtilities;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Tes.ApiClients;
using TesApi.Web.Storage;
using BlobModels = Azure.Storage.Blobs.Models;

namespace TesApi.Web
{
    /// <summary>
    /// Implements caching and retries for <see cref="IAzureProxy"/>.
    /// </summary>
    public class CachingWithRetriesAzureProxy : CachingWithRetriesBase, IAzureProxy
    {
        private readonly IAzureProxy azureProxy;

        /// <summary>
        /// Constructor to create a cache of <see cref="IAzureProxy"/>
        /// </summary>
        /// <param name="azureProxy"><see cref="IAzureProxy"/></param>
        /// <param name="cachingRetryHandler"></param>
        /// <param name="logger"></param>
        public CachingWithRetriesAzureProxy(IAzureProxy azureProxy, CachingRetryPolicyBuilder cachingRetryHandler, ILogger<CachingWithRetriesAzureProxy> logger)
            : base(cachingRetryHandler, logger)
        {
            ArgumentNullException.ThrowIfNull(azureProxy);

            this.azureProxy = azureProxy;
        }


        /// <inheritdoc/>
        async Task IAzureProxy.CreateBatchJobAsync(string jobId, string poolId, CancellationToken cancellationToken)
        {
            try
            {
                await cachingAsyncRetryExceptWhenExists.ExecuteWithRetryAsync(ct => azureProxy.CreateBatchJobAsync(jobId, poolId, ct), cancellationToken);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.JobExists.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        async Task IAzureProxy.AddBatchTasksAsync(IEnumerable<CloudTask> cloudTasks, string jobId, CancellationToken cancellationToken)
        {
            try
            {
                await cachingAsyncRetryExceptWhenExists.ExecuteWithRetryAsync(ct => azureProxy.AddBatchTasksAsync(cloudTasks, jobId, ct), cancellationToken);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.TaskExists.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        async Task IAzureProxy.DeleteBatchJobAsync(string jobId, CancellationToken cancellationToken)
        {
            try
            {
                await cachingAsyncRetryExceptWhenNotFound.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchJobAsync(jobId, ct), cancellationToken);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.JobNotFound.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        async Task IAzureProxy.DeleteBatchTaskAsync(string cloudTaskId, string jobId, CancellationToken cancellationToken)
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
        Task<CloudPool> IAzureProxy.GetBatchPoolAsync(string poolId, CancellationToken cancellationToken, DetailLevel detailLevel) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.GetBatchPoolAsync(poolId, ct, detailLevel), cancellationToken);

        /// <inheritdoc/>
        Task<CloudJob> IAzureProxy.GetBatchJobAsync(string jobId, CancellationToken cancellationToken, DetailLevel detailLevel) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.GetBatchJobAsync(jobId, ct, detailLevel), cancellationToken);

        /// <inheritdoc/>
        async Task IAzureProxy.DeleteBatchComputeNodesAsync(string poolId, IEnumerable<ComputeNode> computeNodes, CancellationToken cancellationToken)
        {
            cachingAsyncRetry.AppCache.Remove($"{nameof(CachingWithRetriesAzureProxy)}:{poolId}");
            await cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.DeleteBatchComputeNodesAsync(poolId, computeNodes, ct), cancellationToken);
        }

        /// <inheritdoc/>
        Task<string> IAzureProxy.DownloadBlobAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.DownloadBlobAsync(blobAbsoluteUri, ct), cancellationToken);

        /// <inheritdoc/>
        Task<bool> IAzureProxy.BlobExistsAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.BlobExistsAsync(blobAbsoluteUri, ct), cancellationToken);

        /// <inheritdoc/>
        IAsyncEnumerable<CloudPool> IAzureProxy.GetActivePoolsAsync(string hostName) => cachingRetry.ExecuteWithRetry(() => azureProxy.GetActivePoolsAsync(hostName));


        /// <inheritdoc/>
        int IAzureProxy.GetBatchActiveJobCount() => cachingRetry.ExecuteWithRetry(azureProxy.GetBatchActiveJobCount);

        /// <inheritdoc/>
        IEnumerable<AzureBatchNodeCount> IAzureProxy.GetBatchActiveNodeCountByVmSize() => cachingRetry.ExecuteWithRetry(azureProxy.GetBatchActiveNodeCountByVmSize);

        /// <inheritdoc/>
        int IAzureProxy.GetBatchActivePoolCount() => cachingRetry.ExecuteWithRetry(azureProxy.GetBatchActivePoolCount);

        /// <inheritdoc/>
        Task<string> IAzureProxy.GetStorageAccountKeyAsync(StorageAccountInfo storageAccountInfo, CancellationToken cancellationToken)
            => cachingAsyncRetry.ExecuteWithRetryAndCachingAsync($"{nameof(CachingWithRetriesAzureProxy)}:{storageAccountInfo.Id}",
                ct => azureProxy.GetStorageAccountKeyAsync(storageAccountInfo, ct), DateTimeOffset.Now.AddHours(1), cancellationToken);

        /// <inheritdoc/>
        async Task<StorageAccountInfo> IAzureProxy.GetStorageAccountInfoAsync(string storageAccountName, CancellationToken cancellationToken)
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
        IAsyncEnumerable<BlobNameAndUri> IAzureProxy.ListBlobsAsync(Uri directoryUri, CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(() => azureProxy.ListBlobsAsync(directoryUri, cancellationToken), cachingRetry);

        /// <inheritdoc/>
        IAsyncEnumerable<Azure.Storage.Blobs.Models.BlobItem> IAzureProxy.ListBlobsWithTagsAsync(Uri containerUri, string prefix, CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(() => azureProxy.ListBlobsWithTagsAsync(containerUri, prefix, cancellationToken), cachingRetry);

        /// <inheritdoc/>
        Task IAzureProxy.SetBlobTags(Uri blobAbsoluteUri, IDictionary<string, string> tags, CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.SetBlobTags(blobAbsoluteUri, tags, ct), cancellationToken);

        /// <inheritdoc/>
        Task IAzureProxy.UploadBlobAsync(Uri blobAbsoluteUri, string content, CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.UploadBlobAsync(blobAbsoluteUri, content, ct), cancellationToken);

        /// <inheritdoc/>
        Task IAzureProxy.UploadBlobFromFileAsync(Uri blobAbsoluteUri, string filePath, CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.UploadBlobFromFileAsync(blobAbsoluteUri, filePath, ct), cancellationToken);

        /// <inheritdoc/>
        Task<BlobModels.BlobProperties> IAzureProxy.GetBlobPropertiesAsync(Uri blobAbsoluteUri, CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.GetBlobPropertiesAsync(blobAbsoluteUri, ct), cancellationToken);

        /// <inheritdoc/>
        string IAzureProxy.GetArmRegion() => azureProxy.GetArmRegion();

        /// <inheritdoc/>
        string IAzureProxy.GetManagedIdentityInBatchAccountResourceGroup(string identityName) => azureProxy.GetManagedIdentityInBatchAccountResourceGroup(identityName);

        /// <inheritdoc/>
        Task<FullBatchPoolAllocationState> IAzureProxy.GetFullAllocationStateAsync(string poolId, CancellationToken cancellationToken)
            => cachingAsyncRetry.ExecuteWithRetryAndCachingAsync(
                $"{nameof(CachingWithRetriesAzureProxy)}:{poolId}",
                ct => azureProxy.GetFullAllocationStateAsync(poolId, ct),
                DateTimeOffset.UtcNow.Add(PoolScheduler.RunInterval.Divide(2)),
                cancellationToken);

        /// <inheritdoc/>
        IAsyncEnumerable<ComputeNode> IAzureProxy.ListComputeNodesAsync(string poolId, DetailLevel detailLevel) => cachingAsyncRetry.ExecuteWithRetryAsync(() => azureProxy.ListComputeNodesAsync(poolId, detailLevel), cachingRetry);

        /// <inheritdoc/>
        IAsyncEnumerable<CloudTask> IAzureProxy.ListTasksAsync(string jobId, DetailLevel detailLevel) => cachingAsyncRetry.ExecuteWithRetryAsync(() => azureProxy.ListTasksAsync(jobId, detailLevel), cachingRetry);

        /// <inheritdoc/>
        Task IAzureProxy.DisableBatchPoolAutoScaleAsync(string poolId, CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.DisableBatchPoolAutoScaleAsync(poolId, ct), cancellationToken);

        /// <inheritdoc/>
        Task IAzureProxy.EnableBatchPoolAutoScaleAsync(string poolId, bool preemptable, TimeSpan interval, IAzureProxy.BatchPoolAutoScaleFormulaFactory formulaFactory, Func<int, ValueTask<int>> currentTargetFunc, CancellationToken cancellationToken) => cachingAsyncRetry.ExecuteWithRetryAsync(ct => azureProxy.EnableBatchPoolAutoScaleAsync(poolId, preemptable, interval, formulaFactory, currentTargetFunc, ct), cancellationToken);
    }
}
