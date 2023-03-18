// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace Tes.Repository
{
    public class ConcurrentDictionaryCache<T> : ICache<T> where T : class
    {
        public const int DefaultMaxMemoryBytes = 1 << 29; // 536,870,912
        public const int DefaultMaxObjectSizeBytes = 4096;  // Assume object uses a max of 4096 byes of memory
        private readonly ConcurrentDictionary<string, T> concurrentDictionary = new ConcurrentDictionary<string, T>();
        private readonly ConcurrentQueue<string> keysToRemove = new ConcurrentQueue<string>();

        // 
        /// <summary>
        /// Total number of items in the cache, defaults to 129,930 tasks (about 2,598 concurrent Mutect2 workflows during scatter).
        /// An ID is a Guid, so also take into account the keysToRemove usage
        /// </summary>
        public int MaxCount { get; set; } = DefaultMaxMemoryBytes / (DefaultMaxObjectSizeBytes + Guid.Empty.ToString().Length);

        public ConcurrentDictionaryCache(IOptions<TaskCacheOptions> options)
        {
            MaxCount = options.Value.MaxMemoryBytes / options.Value.MaxObjectSizeBytes;
        }

        public ConcurrentDictionaryCache(int maxMemoryBytes = DefaultMaxMemoryBytes, int maxObjectSizeBytes = DefaultMaxObjectSizeBytes)
        {
            MaxCount = maxMemoryBytes / maxObjectSizeBytes;
        }

        public int Count()
        {
            return concurrentDictionary.Count;
        }

        public bool TryAdd(string key, T item)
        {
            while (concurrentDictionary.Count > MaxCount - 1) // Don't allow concurrentDictionary to exceed MaxCount on last item add
            {
                // Remove oldest first
                if (keysToRemove.TryDequeue(out string keyToRemove))
                {
                    concurrentDictionary.TryRemove(keyToRemove, out _);
                }
            }

            if (concurrentDictionary.TryAdd(key, item))
            {
                // Only add a key to remove if it was successfully added to the dictionary
                keysToRemove.Enqueue(key);
                return true;
            }

            return false;
        }

        public bool TryGetValue(string key, out T item)
        {
            return concurrentDictionary.TryGetValue(key, out item);
        }

        public bool TryRemove(string key)
        {
            return concurrentDictionary.TryRemove(key, out _);
        }

        public bool TryUpdate(string key, T item)
        {
            // concurrentDictionary.TryUpdate() is not used because its default implementation attempts to compare two objects,
            // which is unnecessary overhead compute for this use case
            concurrentDictionary[key] = item;
            return true;
        }
    }
}
