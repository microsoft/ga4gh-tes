// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
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
        string PoolId { get; }

        /// <summary>
        /// Failures from nodes in <see cref="Microsoft.Azure.Batch.Common.ComputeNodeState.StartTaskFailed"/>.
        /// </summary>
        Queue<StartTaskFailureInformation> StartTaskFailures { get; }

        /// <summary>
        /// Pool allocation failures that impact task execution ability to be successful.
        /// </summary>
        Queue<ResizeError> ResizeErrors { get; }

        /// <summary>
        /// Creates an Azure Batch pool and associated job in the Batch Account.
        /// </summary>
        /// <param name="pool"></param>
        /// <param name="isPreemptible"></param>
        /// <param name="runnerMD5"></param>
        /// <param name="cancellationToken"></param>
        ValueTask CreatePoolAndJobAsync(Azure.ResourceManager.Batch.BatchAccountPoolData pool, bool isPreemptible, string runnerMD5, CancellationToken cancellationToken);

        /// <summary>
        /// Connects to the provided pool and associated job in the Batch Account.
        /// </summary>
        /// <param name="pool">The <see cref="CloudPool"/> to connect to.</param>
        /// <param name="runnerMD5"></param>
        /// <param name="forceRemove"></param>
        /// <param name="cancellationToken"></param>
        ValueTask AssignPoolAsync(CloudPool pool, string runnerMD5, bool forceRemove, CancellationToken cancellationToken);

        /// <summary>
        /// Indicates that the pool is not scheduled to run tasks nor running tasks.
        /// </summary>
        /// <param name="cancellationToken"></param>
        ValueTask<bool> CanBeDeletedAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Indicates that the pool will no longer be serviced.
        /// </summary>
        void MarkRemovedFromService();

        /// <summary>
        /// Updates this instance based on changes to its environment.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <remarks>Calls each internal servicing method in order. Throws all exceptions gathered from all methods.</remarks>
        ValueTask ServicePoolAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Lists <see cref="CloudTask"/>s running in pool's job.
        /// </summary>
        /// <returns></returns>
        IEnumerable<CloudTaskWithPreviousComputeNodeId> ListCloudTasksAsync();

        /// <summary>
        /// Lists <see cref="ComputeNode"/>s that are <see cref="Microsoft.Azure.Batch.Common.ComputeNodeState.Preempted"/> or <see cref="Microsoft.Azure.Batch.Common.ComputeNodeState.Unusable"/>.
        /// </summary>
        /// <returns></returns>
        Task<IAsyncEnumerable<ComputeNode>> ListEjectableComputeNodesAsync();

        /// <summary>
        /// Gets the last time the pool's compute node list was changed.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        ValueTask<DateTime> GetAllocationStateTransitionTimeAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// TesTasks associated with pool by CloudTask.
        /// </summary>
        /// <remarks>Key is <see cref="CloudTask.Id"/>, Value is <see cref="Tes.Models.TesTask.Id"/>.</remarks>
        ConcurrentDictionary<string, string> AssociatedTesTasks { get; }

        /// <summary>
        /// TesTasks associated with pool.
        /// </summary>
        /// <remarks>Key is <see cref="Tes.Models.TesTask.Id"/>, Value is <c>null</c>.</remarks>
        ConcurrentDictionary<string, object> OrphanedTesTasks { get; }

        /// <summary>
        /// A <see cref="CloudTask"/> with a compute node id.
        /// </summary>
        /// <param name="CloudTask">A <see cref="CloudTask"/>.</param>
        /// <param name="PreviousComputeNodeId">A compute node id or null.</param>
        public record struct CloudTaskWithPreviousComputeNodeId(CloudTask CloudTask, string PreviousComputeNodeId);

        /// <summary>
        /// <see cref="TaskFailureInformation"/> paired with compute node Id.
        /// </summary>
        /// <param name="PoolId"><see cref="CloudPool.Id"/>.</param>
        /// <param name="NodeId"><see cref="ComputeNode.Id"/>.</param>
        /// <param name="TaskFailureInformation"><see cref="TaskFailureInformation"/></param>
        public record class StartTaskFailureInformation(string PoolId, string NodeId, TaskFailureInformation TaskFailureInformation);
    }
}
