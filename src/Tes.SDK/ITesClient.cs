﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Models;
using static Tes.SDK.TesClient;

namespace Tes.SDK
{
    public interface ITesClient : IDisposable
    {
        /// <summary>
        /// TBD
        /// </summary>
        string SdkVersion { get; }

        /// <summary>
        /// Cancels an existing TES task by id
        /// </summary>
        /// <param name="taskId">The TES task's ID</param>
        /// <param name="cancellationToken">The cancellationToken</param>
        /// <returns></returns>
        Task CancelTaskAsync(string taskId, CancellationToken cancellationToken = default);

        /// <summary>
        /// Creates a new TES task and blocks forever until it's done
        /// </summary>
        /// <param name="tesTask">The TES task to create</param>
        /// <param name="cancellationToken">The cancellationToken</param>
        /// <returns>The created TES task</returns>
        Task<TesTask> CreateAndWaitTilDoneAsync(TesTask tesTask, CancellationToken cancellationToken = default);

        /// <summary>
        /// Downloads the logs for a TES task
        /// </summary>
        /// <param name="tesTask">The TES task to get logs for</param>
        /// <param name="storageAccountName">The default TES storage account name</param>
        /// <param name="cancellationToken">The cancellationToken</param>
        /// <returns></returns>
        Task<Dictionary<TesLogType, string>> DownloadLogsAsync(TesTask tesTask, string storageAccountName, CancellationToken cancellationToken);

        /// <summary>
        /// Creates a new TES task
        /// </summary>
        /// <param name="tesTask">The TES task to create</param>
        /// <param name="cancellationToken">The cancellationToken</param>
        /// <returns>The created TES task's ID</returns>
        Task<string> CreateTaskAsync(TesTask tesTask, CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets an existing TES task by id
        /// </summary>
        /// <param name="taskId">The TES task's ID</param>
        /// <param name="view">The portion of the task's metadata to retrieve.</param>
        /// <param name="cancellationToken">The cancellationToken</param>
        /// <returns>The TES task</returns>
        Task<TesTask> GetTaskAsync(string taskId, TesView view = TesView.MINIMAL, CancellationToken cancellationToken = default);

        /// <summary>
        /// Enumerates all TES tasks
        /// </summary>
        /// <param name="view">The portion of each task's metadata to retrieve.</param>
        /// <param name="cancellationToken">The cancellationToken</param>
        /// <returns>The TES tasks</returns>
        IAsyncEnumerable<TesTask> ListTasksAsync(TaskQueryOptions? options = null, CancellationToken cancellationToken = default);
    }
}
