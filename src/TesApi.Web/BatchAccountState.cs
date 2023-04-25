using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.Batch;

namespace TesApi.Web
{
    /// <summary>
    /// Gets the combined state of Azure Batch jobs, tasks, pools, and compute nodes that correspond to this TES deployment
    /// </summary>
    public readonly struct BatchAccountState
    {
        /// <summary>
        /// Creates a <see cref="BatchAccountState"/>.
        /// </summary>
        /// <param name="pools">Enumeration of <see cref="CloudPool"/>.</param>
        /// <param name="jobs">Enumeration of <see cref="CloudJob"/>.</param>
        /// <param name="nodesFunc">Function that returns an enumeration of <see cref="ComputeNode"/> attached to a <see cref="CloudPool"/>.</param>
        /// <param name="tasksFunc">Function that returns an enumeration of <see cref="CloudTask"/> attached to a <see cref="CloudJob"/>.</param>
        public BatchAccountState(IAsyncEnumerable<CloudPool> pools, IAsyncEnumerable<CloudJob> jobs, Func<CloudPool, IAsyncEnumerable<ComputeNode>> nodesFunc, Func<CloudJob, IAsyncEnumerable<CloudTask>> tasksFunc)
        {
            ArgumentNullException.ThrowIfNull(nodesFunc);
            ArgumentNullException.ThrowIfNull(tasksFunc);
            ArgumentNullException.ThrowIfNull(pools);
            ArgumentNullException.ThrowIfNull(jobs);

            JobsAndTasks = CreateDictionary(jobs, tasksFunc);
            PoolsAndNodes = CreateDictionary(pools, nodesFunc);

            static IReadOnlyDictionary<TKey, IReadOnlyList<TValue>> CreateDictionary<TKey, TValue>(IAsyncEnumerable<TKey> keys, Func<TKey, IAsyncEnumerable<TValue>> func)
            {
                var dictionary = new Dictionary<TKey, IReadOnlyList<TValue>>();

                foreach (var key in keys.ToEnumerable())
                {
                    dictionary[key] = func(key).ToEnumerable().ToList().AsReadOnly();
                }

                return dictionary.AsReadOnly();
            }
        }

        /// <summary>
        /// The <see cref="CloudPool"/>s, along with their attached <see cref="ComputeNode"/>s.
        /// </summary>
        public IReadOnlyDictionary<CloudPool, IReadOnlyList<ComputeNode>> PoolsAndNodes { get; }

        /// <summary>
        /// The <see cref="CloudJob"/>s, along with their attached <see cref="CloudTask"/>s.
        /// </summary>
        public IReadOnlyDictionary<CloudJob, IReadOnlyList<CloudTask>> JobsAndTasks { get; }
    }
}
