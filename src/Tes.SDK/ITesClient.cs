// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Models;

namespace Tes.SDK
{
    public interface ITesClient
    {
        string SdkVersion { get; }

        Task CancelTaskAsync(string taskId, CancellationToken cancellationToken = default);
        Task<TesTask> CreateAndWaitTilDoneAsync(TesTask tesTask, CancellationToken cancellationToken = default);
        Task<string> CreateTaskAsync(TesTask tesTask, CancellationToken cancellationToken = default);
        Task<TesTask> GetTaskAsync(string taskId, TesView view = TesView.MINIMAL, CancellationToken cancellationToken = default);
        IAsyncEnumerable<TesTask> ListTasksAsync(CancellationToken cancellationToken = default);
    }
}
