// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using LazyCache;

namespace Tes.Repository
{
    /// <summary>
    /// A wrapper for LazyCache.IAppCache
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class TesRepositoryCache<T> : ICache<T> where T : class
    {
        private readonly Microsoft.Extensions.Caching.Memory.MemoryCacheEntryOptions defaultItemOptions = new() { SlidingExpiration = System.TimeSpan.FromDays(1) };
        private readonly IAppCache cache;
        private object _lock = new();

        static TesRepositoryCache() => Utilities.NewtonsoftJsonSafeInit.SetDefaultSettings();

        /// <summary>
        /// Default constructor expecting the singleton IDistributedCache
        /// </summary>
        /// <param name="appCache"></param>
        public TesRepositoryCache(IAppCache appCache)
        {
            cache = appCache;
        }

        private string Key(string key) => $"{nameof(TesRepositoryCache<T>)}:{key}";

        /// <inheritdoc/>
        public int MaxCount { get => throw new System.NotSupportedException(); set => throw new System.NotSupportedException(); }

        /// <inheritdoc/>
        public int Count()
        {
            throw new System.NotSupportedException();
        }

        /// <inheritdoc/>
        public bool TryAdd(string key, T task)
        {
            lock (_lock)
            {
                cache.Add(Key(key), task, defaultItemOptions);
                return true;
            }
        }

        /// <inheritdoc/>
        public bool TryGetValue(string key, out T task)
        {
            lock (_lock)
            {
                return cache.TryGetValue(Key(key), out task);
            }
        }

        /// <inheritdoc/>
        public bool TryRemove(string key)
        {
            var cacheKey = Key(key);

            lock (_lock)
            {
                if (cache.Get<T>(cacheKey) is null)
                {
                    return false;
                }

                cache.Remove(cacheKey);
                return true;
            }
        }

        /// <inheritdoc/>
        public bool TryUpdate(string key, T task, System.TimeSpan expiration)
        {
            var cacheKey = Key(key);
            lock (_lock)
            {
                cache.Remove(cacheKey);
                cache.Add(cacheKey, task, expiration == default ? defaultItemOptions : LazyCacheEntryOptions.WithImmediateAbsoluteExpiration(expiration));
                return true;
            }
        }
    }
}
