// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web
{
    /// <summary>
    /// Combines a <see cref="Tes.Models.TesTask"/> with a <see cref="System.Threading.Tasks.Task{TResult}"/>.
    /// </summary>
    /// <param name="Task"> The wrapped <see cref="System.Threading.Tasks.Task{TResult}"/>.</param>
    /// <param name="TesTask"> The wrapped <see cref="Tes.Models.TesTask"/>.</param>
    /// <typeparam name="TResult">The type of the result produced by the <see cref="System.Threading.Tasks.Task{TResult}"/>.</typeparam>
    public record class TesTaskTask<TResult>(System.Threading.Tasks.Task<TResult> Task, Tes.Models.TesTask TesTask) : System.IDisposable
    {
        /// <summary>Gets an awaiter used to await the <see cref="System.Threading.Tasks.Task{TResult}"/>.</summary>
        /// <returns>An awaiter instance.</returns>
        // https://devblogs.microsoft.com/pfxteam/await-anything/
        public System.Runtime.CompilerServices.TaskAwaiter<TResult> GetAwaiter()
        {
            return Task.GetAwaiter();
        }

        /// <summary>
        /// Disposes the member <see cref="Task"/>, releasing all of its unmanaged resources.
        /// </summary>
        /// <remarks>
        /// Unlike most of the members of the member <see cref="Task"/>, this method is not thread-safe.
        /// Also, <see cref="Dispose()"/> may only be called on a member <see cref="Task"/> that is in one of
        /// the final states: <see cref="System.Threading.Tasks.TaskStatus.RanToCompletion">RanToCompletion</see>,
        /// <see cref="System.Threading.Tasks.TaskStatus.Faulted">Faulted</see>, or
        /// <see cref="System.Threading.Tasks.TaskStatus.Canceled">Canceled</see>.
        /// </remarks>
        /// <exception cref="System.InvalidOperationException">
        /// The exception that is thrown if the member <see cref="Task"/> is not in
        /// one of the final states: <see cref="System.Threading.Tasks.TaskStatus.RanToCompletion">RanToCompletion</see>,
        /// <see cref="System.Threading.Tasks.TaskStatus.Faulted">Faulted</see>, or
        /// <see cref="System.Threading.Tasks.TaskStatus.Canceled">Canceled</see>.
        /// </exception>
#pragma warning disable CA1816 // Dispose methods should call SuppressFinalize
        public void Dispose()
#pragma warning restore CA1816 // Dispose methods should call SuppressFinalize
        {
            Task.Dispose();
        }
    }
}
