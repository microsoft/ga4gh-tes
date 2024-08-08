﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading;
using System.Threading.Tasks;
using Azure.ResourceManager.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Logging;
using Tes.ApiClients;

namespace TesApi.Web.Management.Batch
{
    /// <summary>
    /// Implements caching and retries for <see cref="IBatchPoolManager"/>.
    /// </summary>
    public class CachingWithRetriesBatchPoolManager : CachingWithRetriesBase, IBatchPoolManager
    {
        private readonly IBatchPoolManager batchPoolManager;

        /// <summary>
        /// Constructor to create a cache of <see cref="IBatchPoolManager"/>
        /// </summary>
        /// <param name="batchPoolManager"></param>
        /// <param name="cachingRetryHandler"></param>
        /// <param name="logger"></param>
        public CachingWithRetriesBatchPoolManager(IBatchPoolManager batchPoolManager, CachingRetryPolicyBuilder cachingRetryHandler, ILogger<CachingWithRetriesBatchPoolManager> logger)
            : base(cachingRetryHandler, logger)
        {
            ArgumentNullException.ThrowIfNull(batchPoolManager);
            ArgumentNullException.ThrowIfNull(cachingRetryHandler);

            this.batchPoolManager = batchPoolManager;
        }


        /// <inheritdoc/>
        async Task IBatchPoolManager.DeleteBatchPoolAsync(string poolId, CancellationToken cancellationToken)
        {
            try
            {
                await cachingAsyncRetryExceptWhenNotFound.ExecuteWithRetryAsync(ct => batchPoolManager.DeleteBatchPoolAsync(poolId, ct), cancellationToken);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.TaskNotFound.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            { }
        }

        /// <inheritdoc/>
        async Task<string> IBatchPoolManager.CreateBatchPoolAsync(BatchAccountPoolData poolSpec, bool isPreemptable, CancellationToken cancellationToken)
        {
            try
            {
                return await cachingAsyncRetryExceptWhenExists.ExecuteWithRetryAsync(ct => batchPoolManager.CreateBatchPoolAsync(poolSpec, isPreemptable, ct), cancellationToken);
            }
            catch (BatchException exc) when (BatchErrorCodeStrings.PoolExists.Equals(exc.RequestInformation?.BatchError?.Code, StringComparison.OrdinalIgnoreCase))
            {
                return poolSpec.Name;
            }
        }
    }
}
