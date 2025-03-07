// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Tes.Models;
using TesApi.Web.Runner;

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
        Task UploadTaskRunnerIfNeededAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Updates the <see cref="TesTask"/> with task-related state on a batch system
        /// </summary>
        /// <param name="tesTask">The TES task</param>
        /// <param name="taskState">Current Azure Batch task state info</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>True if the TES task was changed.</returns>
        ValueTask<bool> ProcessTesTaskBatchStateAsync(TesTask tesTask, AzureBatchTaskState taskState, CancellationToken cancellationToken);

        /// <summary>
        /// Schedules a <see cref="TesTask"/>s on a batch system
        /// </summary>
        /// <param name="tesTask">A <see cref="TesTask"/> to schedule on the batch system.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>True to persist the <see cref="TesTask"/>, otherwise False.</returns>
        Task<bool> ProcessQueuedTesTaskAsync(TesTask tesTask, CancellationToken cancellationToken);

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
        Task DeletePoolAndJobAsync(IBatchPool pool, CancellationToken cancellationToken);

        /// <summary>
        /// Removes pool from list of managed pools.
        /// </summary>
        /// <param name="pool">Pool to remove.</param>
        /// <returns></returns>
        bool RemovePoolFromList(IBatchPool pool);

        /// <summary>
        /// Flushes empty pools to accommodate pool quota limits.
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
        /// Performs background tasks expected to complete in less than a second in aggregate.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        ValueTask PerformShortBackgroundTasksAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Performs background tasks expected to take longer than a second.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        IAsyncEnumerable<Task> PerformLongBackgroundTasksAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Generates refreshed start task.
        /// </summary>
        /// <param name="poolId"></param>
        /// <param name="startTaskNodeFile"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        ValueTask PatchBatchPoolStartTaskCommandline(string poolId, Uri startTaskNodeFile, CancellationToken cancellationToken);

        /// <summary>
        /// Identifies an azure cloud task.
        /// </summary>
        /// <param name="JobId"><see cref="CloudJob.Id"/> that contains the task.</param>
        /// <param name="TaskId"><see cref="CloudTask.Id"/>.</param>
        /// <param name="Created"><see cref="CloudTask.CreationTime"/></param>
        public record struct CloudTaskId(string JobId, string TaskId, DateTime Created);

        /// <summary>
        /// TES metadata carried in the batch pool.
        /// </summary>
        /// <param name="HostName"><see cref="Options.BatchSchedulingOptions.Prefix"/>.</param>
        /// <param name="IsDedicated">Compute nodes in pool are not preemptible.</param>
        /// <param name="RunnerMD5">NodeTaskRunner hash.</param>
        /// <param name="StartTaskUri">URL of start task node file.</param>
        /// <param name="EventsVersion"><see cref="Events.RunnerEventsMessage.EventsVersion"/>.</param>
        record struct PoolMetadata(string HostName, bool IsDedicated, string RunnerMD5, Uri StartTaskUri, IDictionary<string, object> EventsVersion)
        {
            private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

            /// <inheritdoc/>
            public override readonly string ToString()
            {
                return JsonSerializer.Serialize(this, Options);
            }

            /// <summary>
            /// Create <see cref="PoolMetadata"/> from batch pool metadata value.
            /// </summary>
            /// <param name="value">Batch pool '<see cref="BatchScheduler.PoolMetadata"/>' metadata value.</param>
            /// <returns><see cref="PoolMetadata"/>.</returns>
            public static PoolMetadata Create(string value)
            {
                return JsonSerializer.Deserialize<PoolMetadata>(value, Options);
            }

            internal readonly bool Validate(bool validateEventsVersion)
            {
                if (string.IsNullOrWhiteSpace(HostName) || string.IsNullOrWhiteSpace(RunnerMD5))
                {
                    return false;
                }

                return validateEventsVersion
                    ? !NormalizeForValidation(Events.RunnerEventsMessage.EventsVersion).OrderBy(pair => pair.Key, StringComparer.OrdinalIgnoreCase)
                        .SequenceEqual(NormalizeForValidation(EventsVersion).OrderBy(pair => pair.Key, StringComparer.OrdinalIgnoreCase))
                    : Events.RunnerEventsMessage.EventsVersion.Keys.Order(StringComparer.OrdinalIgnoreCase)
                        .SequenceEqual(EventsVersion.Keys.Order(StringComparer.OrdinalIgnoreCase), StringComparer.OrdinalIgnoreCase);
            }

            static Dictionary<string, object> NormalizeForValidation(IDictionary<string, object> value)
            {
                return value
                    .OrderBy(pair => pair.Key)
                    .Select(static pair => new KeyValuePair<string, object>(pair.Key.ToUpperInvariant(), pair.Value switch
                    {
                        JsonElement element when JsonValueKind.Number.Equals(element.ValueKind) => new Version(element.ToString()),
                        JsonElement element when JsonValueKind.String.Equals(element.ValueKind) => new Version(element.ToString()),
                        string stringValue => new Version(stringValue),
                        _ => pair.Value
                    }))
                    .ToDictionary();
            }
        }
    }
}
