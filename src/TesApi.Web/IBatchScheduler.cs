// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Tes.Models;

namespace TesApi.Web
{
    /// <summary>
    /// An interface for scheduling <see cref="TesTask"/>s on a batch processing system
    /// </summary>
    public interface IBatchScheduler
    {
        /// <summary>
        /// Flag indicating that empty pools need to be flushed because pool quota has been reached.
        /// </summary>
        bool NeedPoolFlush { get; }

        /// <summary>
        /// Loads existing pools from Azure Batch Account tagged with this instance's "Name"
        /// </summary>
        /// <returns></returns>
        /// <remarks>This should be called only once after the <see cref="BatchScheduler"/> is created before any other methods are called.</remarks>
        Task LoadExistingPoolsAsync();

        /// <summary>
        /// Schedule an enumeration of <see cref="TesTask"/> on a batch system until completion or failure
        /// </summary>
        /// <param name="tesTasks">An enumeration of <see cref="TesTask"/> to schedule on the batch system</param>
        /// <param name="cancellationToken"></param>
        /// <returns>Whether each <see cref="TesTask"/> was modified.</returns>
        IAsyncEnumerable<(TesTask TesTask, Task<bool> IsChangedAsync)> ProcessTesTasksAsync(IEnumerable<TesTask> tesTasks, CancellationToken cancellationToken = default);

        /// <summary>
        /// Adds <see cref="IBatchPool"/> to the managed batch pools.
        /// </summary>
        /// <param name="pool">The pool to add to the pools accessible by this scheduler.</param>
        /// <returns>True if pool was added, false otherwise (including if it was already present).</returns>
        bool AddPool(IBatchPool pool);

        /// <summary>
        /// Enumerates all the managed batch pools.
        /// </summary>
        /// <returns></returns>
        IEnumerable<IBatchPool> GetPools();

        /// <summary>
        /// Deletes pool and job.
        /// </summary>
        /// <param name="pool"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task DeletePoolAsync(IBatchPool pool, CancellationToken cancellationToken);

        /// <summary>
        /// Retrieves pools associated with this TES from the batch account.
        /// </summary>
        /// <returns></returns>
        IAsyncEnumerable<CloudPool> GetCloudPools();

        /// <summary>
        /// Removes pool from list of managed pools.
        /// </summary>
        /// <param name="pool">Pool to remove.</param>
        /// <returns></returns>
        bool RemovePoolFromList(IBatchPool pool);

        /// <summary>
        /// Garbage collects the old batch task state log hashset
        /// </summary>
        void ClearBatchLogState();

        /// <summary>
        /// Flushes empty pools to accomodate pool quota limits.
        /// </summary>
        /// <param name="assignedPools">Pool Ids of pools connected to active TES Tasks. Used to prevent accidentally removing active pools.</param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        ValueTask FlushPoolsAsync(IEnumerable<string> assignedPools, CancellationToken cancellationToken);
    }
}
