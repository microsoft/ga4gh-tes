// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace TesApi.Web.Extensions
{
    /// <summary>
    /// Extension methods for <see cref="System.Threading.Tasks.Task"/> and associated types
    /// </summary>
    public static class SystemThreadingTaskExtensions
    {
        /// <summary>
        /// Streams items as their associated tasks complete.
        /// </summary>
        /// <typeparam name="T">Type of items in <paramref name="source"/>.</typeparam>
        /// <param name="source">Items to be streamed in the order of completion of their associated <see cref="Task"/>s.</param>
        /// <param name="cancellationToken">Required parameter. Must not be <see cref="CancellationToken.None"/>.</param>
        /// <param name="sourceToTask">Required if <typeparamref name="T"/> is not derived from <see cref="Task"/>. Obtains the <see cref="Task"/> associated with each element in <paramref name="source"/>.</param>
        /// <returns><paramref name="source"/> in the order of the completion of their associated <see cref="Task"/>s.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="ArgumentException"></exception>
        /// <remarks>
        /// A task is sent to the return enumeration when it is "complete", which is when it either completes successfully, fails (queues an exception), or is cancelled.<br/>
        /// No items in <paramref name="source"/> should share an identical <see cref="Task"/> instance.
        /// </remarks>
        public static async IAsyncEnumerable<T> WhenEach<T>(this IEnumerable<T> source, [EnumeratorCancellation] CancellationToken cancellationToken, Func<T, Task> sourceToTask = default)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(cancellationToken);

            // Ensure we have a usable sourceToTask
            sourceToTask ??= typeof(T).IsAssignableTo(typeof(Task)) ? new(i => (i as Task)!) : throw new ArgumentNullException(nameof(sourceToTask));

            var list = source.Where(e => e is not null).Select(e => (Entry: e, Task: sourceToTask(e))).ToList();
            var pendingCount = list.Count;

            //if (list.Select(e => e.Task).ToHashSet().Count != pendingCount) // Check for duplicate tasks
            //{
            //    throw new ArgumentException("Duplicate System.Threading.Tasks found referenced in collection.", nameof(source));
            //}

            if (list.Count == 0)
            {
                yield break;
            }

            // There should be no more ArgumentExceptions after this point.
            var channel = Channel.CreateBounded<T>(pendingCount);

            // Add continuations to every task. Those continuations will feed the foreach below
            _ = Parallel.ForEach(list, tuple =>
            {
                // The continuation task returned with ContinueWith() is attached to the associated task and will be disposed with it.
                _ = tuple.Task.ContinueWith(task =>
                {
                    _ = channel.Writer.TryWrite(tuple.Entry);

                    if (Interlocked.Decrement(ref pendingCount) == 0)
                    {
                        channel.Writer.Complete();
                    }
                },
                cancellationToken,
                TaskContinuationOptions.DenyChildAttach,
                TaskScheduler.Default);
            });

            // Return all completed entries as their tasks are completed, no matter if by failure, cancellation, or running to completion.
            await foreach (var entry in channel.Reader.ReadAllAsync(cancellationToken))
            {
                yield return entry;
            }
        }
    }
}
