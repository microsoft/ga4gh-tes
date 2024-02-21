// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading;
using System.Threading.Tasks;
using Azure.ResourceManager.Batch;
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
        string PoolId { get; }

        /// <summary>
        /// Creates an Azure Batch pool and associated job in the Batch Account.
        /// </summary>
        /// <param name="pool"></param>
        /// <param name="isPreemptible"></param>
        /// <param name="cancellationToken"></param>
        ValueTask CreatePoolAndJobAsync(BatchAccountPoolData pool, bool isPreemptible, CancellationToken cancellationToken);

        /// <summary>
        /// Connects to the provided pool and associated job in the Batch Account.
        /// </summary>
        /// <param name="pool">The <see cref="CloudPool"/> to connect to.</param>
        /// <param name="cancellationToken"></param>
        ValueTask AssignPoolAsync(CloudPool pool, CancellationToken cancellationToken);

        /// <summary>
        /// Indicates that the pool is not scheduled to run tasks nor running tasks.
        /// </summary>
        /// <param name="cancellationToken"></param>
        ValueTask<bool> CanBeDeleted(CancellationToken cancellationToken = default);

        /// <summary>
        /// Removes and returns the next available resize error.
        /// </summary>
        /// <returns>The first <see cref="ResizeError"/> in the list, or null if the list is empty.</returns>
        /// <remarks><see cref="ResizeError.Values"/> appears to contain two entries with <see cref="NameValuePair.Name"/> containing respectively "code" &amp; "message"</remarks>
        ResizeError PopNextResizeError();

        /// <summary>
        /// Removes and returns the next available start task failure.
        /// </summary>
        /// <returns>The first <see cref="TaskFailureInformation"/> in the list, or null if the list is empty.</returns>
        TaskFailureInformation PopNextStartTaskFailure();

        /// <summary>
        /// Updates this instance based on changes to its environment.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <remarks>Calls each internal servicing method in order. Throws all exceptions gathered from all methods.</remarks>
        ValueTask ServicePoolAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets the last time the pool's compute node list was changed.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        ValueTask<DateTime> GetAllocationStateTransitionTime(CancellationToken cancellationToken = default);
    }
}
