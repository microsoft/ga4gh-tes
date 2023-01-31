// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using BatchModels = Microsoft.Azure.Management.Batch.Models;

namespace TesApi.Web.Management.Batch
{
    /// <summary>
    /// 
    /// </summary>
    public interface IBatchPoolManager
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="poolInfo"></param>
        /// <param name="isPreemptable"></param>
        /// <returns></returns>
        Task<PoolInformation> CreateBatchPoolAsync(BatchModels.Pool poolInfo, bool isPreemptable);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="poolName"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task DeleteBatchPoolAsync(string poolName, CancellationToken cancellationToken = default);

    }
}
