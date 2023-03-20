using System;
using Microsoft.Extensions.Caching.Memory;

namespace Tes.Repository
{
    public class MemoryCache<T> : ICache<T> where T : class
    {
        private const int defaultMaxObjectSizeBytes = 4096;
        private const int defaultMaxMemoryBytes = 1 << 29; // 536,870,912

        /// <summary>
        /// A TesTask can run for 7 days, and hypothetically there could be weeks of queued tasks, so set a long default
        /// </summary>
        /// 
        private static TimeSpan defaultItemExpiration = TimeSpan.FromDays(30); 
        private IMemoryCache cache;

        public int MaxCount { get; set; } = defaultMaxMemoryBytes / (defaultMaxObjectSizeBytes + Guid.Empty.ToString().Length);

        public MemoryCache(IMemoryCache cache, int maxMemory = defaultMaxMemoryBytes, int maxObjectSize = defaultMaxObjectSizeBytes)
        {
            MaxCount = maxMemory / maxObjectSize;
            this.cache = cache;
        }

        public int Count()
        {
            // IMemoryCache does not support getting the Count()            
            throw new InvalidOperationException();
        }

        public bool TryAdd(string key, T task)
        {
            cache.Set(key, task, defaultItemExpiration);
            return true;
        }

        public bool TryGetValue(string key, out T task)
        {
            return cache.TryGetValue(key, out task);
        }

        public bool TryRemove(string key)
        {
            cache.Remove(key);
            return true;
        }

        public bool TryUpdate(string key, T task)
        {
            return TryAdd(key, task);
        }
    }
}
