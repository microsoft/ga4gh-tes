// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
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
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        /// <remarks>This should be called only once after the <see cref="BatchScheduler"/> is created before any other methods are called.</remarks>
        Task LoadExistingPoolsAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Stores the compute node task runner in the default storage account
        /// </summary>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        /// <remarks>This should be called only once after the <see cref="BatchScheduler"/> is created before any other methods are called.</remarks>
        Task UploadTaskRunnerIfNeeded(CancellationToken cancellationToken);

        /// <summary>
        /// Update <see cref="TesTask"/>s with task-related state on a batch system
        /// </summary>
        /// <param name="tesTasks"><see cref="TesTask"/>s to schedule on the batch system.</param>
        /// <param name="taskStates"><see cref="AzureBatchTaskState"/>s corresponding to each <seealso cref="TesTask"/>.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>True for each corresponding <see cref="TesTask"/> that needs to be persisted.</returns>
        IAsyncEnumerable<RelatedTask<TesTask, bool>> ProcessTesTaskBatchStatesAsync(IEnumerable<TesTask> tesTasks, AzureBatchTaskState[] taskStates, CancellationToken cancellationToken);

        /// <summary>
        /// Schedule queued <see cref="TesTask"/>s on a batch system
        /// </summary>
        /// <param name="tesTasks"><see cref="TesTask"/>s to schedule on the batch system.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>True for each <see cref="TesTask"/> that needs to be persisted.</returns>
        IAsyncEnumerable<RelatedTask<TesTask, bool>> ProcessQueuedTesTasksAsync(TesTask[] tesTasks, CancellationToken cancellationToken);

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
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        Task DeletePoolAsync(IBatchPool pool, CancellationToken cancellationToken);

        /// <summary>
        /// Removes pool from list of managed pools.
        /// </summary>
        /// <param name="pool">Pool to remove.</param>
        /// <returns></returns>
        bool RemovePoolFromList(IBatchPool pool);

        /// <summary>
        /// Flushes empty pools to accomodate pool quota limits.
        /// </summary>
        /// <param name="assignedPools">Pool Ids of pools connected to active TES Tasks. Used to prevent accidentally removing active pools.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        ValueTask FlushPoolsAsync(IEnumerable<string> assignedPools, CancellationToken cancellationToken);

        /// <summary>
        /// Gets the <see cref="TesTask.Id"/> from a <see cref="CloudTask.Id"/>.
        /// </summary>
        /// <param name="cloudTaskId"><see cref="CloudTask.Id"/>.</param>
        /// <returns><see cref="TesTask.Id"/>.</returns>
        string GetTesTaskIdFromCloudTaskId(string cloudTaskId);

        /// <summary>
        /// Deletes azure batch tasks.
        /// </summary>
        /// <param name="tasks"><see cref="CloudTaskId"/>s to delete from the batch system.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>True for each <see cref="CloudTaskId"/> that was either deleted or not found.</returns>
        IAsyncEnumerable<RelatedTask<CloudTaskId, bool>> DeleteCloudTasksAsync(IAsyncEnumerable<CloudTaskId> tasks, CancellationToken cancellationToken);

        /// <summary>
        /// Gets unprocessed events from the storage account.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="event">Optional event to retrieve. Defaults to all events.</param>
        /// <returns></returns>
        IAsyncEnumerable<Events.RunnerEventsMessage> GetEventMessagesAsync(CancellationToken cancellationToken, string @event = default);

        /// <summary>
        /// Identifies an azure cloud task.
        /// </summary>
        /// <param name="JobId"><see cref="CloudJob.Id"/> that contains the task.</param>
        /// <param name="TaskId"><see cref="CloudTask.Id"/>.</param>
        /// <param name="Created"><see cref="CloudTask.CreationTime"/></param>
        public record struct CloudTaskId(string JobId, string TaskId, DateTime Created);
    }
}
