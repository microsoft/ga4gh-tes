// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using BatchModels = Microsoft.Azure.Management.Batch.Models;

namespace TesApi.Web.Management.Batch
{
    /// <summary>
    /// Provides management plane operations for Azure Batch Pools
    /// </summary>
    public interface IBatchPoolManager
    {
        /// <summary>
        /// Creates an Azure Batch pool who's lifecycle must be manually managed
        /// </summary>
        /// <param name="poolInfo">Contains information about the pool. <see cref="BatchModels.ProxyResource.Name"/> becomes the <see cref="CloudPool.Id"/></param>
        /// <param name="isPreemptable">True if nodes in this pool will all be preemptable. False if nodes will all be dedicated.</param>
        /// <returns><see cref="PoolInformation"/> that identifies the created pool.</returns>
        Task<PoolInformation> CreateBatchPoolAsync(BatchModels.Pool poolInfo, bool isPreemptable);

        /// <summary>
        /// Deletes the specified pool
        /// </summary>
        /// <param name="poolName">The id of the pool.</param>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        Task DeleteBatchPoolAsync(string poolName, CancellationToken cancellationToken = default);
    }
}
