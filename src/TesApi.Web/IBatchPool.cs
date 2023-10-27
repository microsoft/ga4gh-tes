// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;

namespace TesApi.Web
{
    /// <summary>
    /// Represents a pool in an Azure Batch Account.
    /// </summary>
    public interface IBatchPool
    {
        /// <summary>
        /// Indicates that the pool is available for new jobs/tasks.
        /// </summary>
        bool IsAvailable { get; }

        /// <summary>
        /// Provides the <see cref="CloudPool.Id"/> for the pool.
        /// </summary>
        string Id { get; }

        /// <summary>
        /// Creates an Azure Batch pool and associated job in the Batch Account.
        /// </summary>
        /// <param name="pool"></param>
        /// <param name="isPreemptible"></param>
        /// <param name="cancellationToken"></param>
        ValueTask CreatePoolAndJobAsync(Microsoft.Azure.Management.Batch.Models.Pool pool, bool isPreemptible, CancellationToken cancellationToken);

        /// <summary>
        /// Connects to the provided pool and associated job in the Batch Account.
        /// </summary>
        /// <param name="pool">The <see cref="CloudPool"/> to connect to.</param>
        /// <param name="forceRemove"></param>
        /// <param name="cancellationToken"></param>
        ValueTask AssignPoolAsync(CloudPool pool, bool forceRemove, CancellationToken cancellationToken);

        /// <summary>
        /// Indicates that the pool is not scheduled to run tasks nor running tasks.
        /// </summary>
        /// <param name="cancellationToken"></param>
        ValueTask<bool> CanBeDeleted(CancellationToken cancellationToken = default);

        /// <summary>
        /// Updates this instance based on changes to its environment.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <remarks>Calls each internal servicing method in order. Throws all exceptions gathered from all methods.</remarks>
        ValueTask ServicePoolAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets nonrecoverable compute node related failures that occur before tasks are assigned to compute nodes.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        IAsyncEnumerable<CloudTaskBatchTaskState> GetTaskResizeFailures(CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets the last time the pool's compute node list was changed.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        ValueTask<DateTime> GetAllocationStateTransitionTime(CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets the completed tasks in this pool's associated job.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        IAsyncEnumerable<CloudTask> GetCompletedTasks(CancellationToken cancellationToken);

        /// <summary>
        /// A <see cref="CloudTask"/> not yet assigned a compute nodes to remove due to a nonrecoverable compute node or pool resize error.
        /// </summary>
        /// <param name="CloudTaskId">A <see cref="CloudTask"/>s not yet assigned a compute node.</param>
        /// <param name="TaskState">A compute node and/or pool resize error.</param>
        public record CloudTaskBatchTaskState(string CloudTaskId, AzureBatchTaskState TaskState);
    }
}
