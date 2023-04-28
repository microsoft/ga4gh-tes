// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Caching.Distributed;

namespace Tes.Repository
{
    /// <summary>
    /// A wrapper for LazyCache.IAppCache
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class TesRepositoryCache<T> : ICache<T> where T : class
    {
        /// <summary>
        /// A TesTask can run for 7 days, and hypothetically there could be weeks of queued tasks, so set a long default
        /// </summary>
        private static readonly System.TimeSpan defaultItemExpiration = System.TimeSpan.FromDays(30);
        private static readonly System.TimeSpan removedItemExpiration = System.TimeSpan.FromMinutes(60);

        private readonly IDistributedCache cache;
        private readonly DistributedCacheEntryOptions entryOptions = new() { SlidingExpiration = defaultItemExpiration };
        private readonly DistributedCacheEntryOptions removedEntryOptions = new() { SlidingExpiration = removedItemExpiration };

        /// <summary>
        /// Default constructor expecting the singleton LazyCache.IAppCache
        /// </summary>
        /// <param name="appCache"></param>
        public TesRepositoryCache(IDistributedCache appCache)
        {
            cache = appCache;
        }

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
            cache.Set($"{nameof(TesRepositoryCache<T>)}:{key}", Convert(task), entryOptions);
            return true;
        }

        /// <inheritdoc/>
        public bool TryGetValue(string key, out T task)
        {
            task = Convert(cache.Get($"{nameof(TesRepositoryCache<T>)}:{key}"));
            return task is not null;
        }

        /// <inheritdoc/>
        public bool TryRemove(string key)
        {
            var cacheKey = $"{nameof(TesRepositoryCache<T>)}:{key}";
            var item = cache.Get(cacheKey);

            if (item is null)
            {
                return false;
            }

            cache.Set(cacheKey, item, removedEntryOptions);
            return true;
        }

        /// <inheritdoc/>
        public bool TryUpdate(string key, T task)
        {
            var cacheKey = $"{nameof(TesRepositoryCache<T>)}:{key}";
            cache.Remove(cacheKey);
            return TryAdd(key, task);
        }

        static byte[] Convert(T value)
            => value is null ? null : System.Text.Encoding.UTF8.GetBytes(Newtonsoft.Json.JsonConvert.SerializeObject(value));

        static T Convert(byte[] value)
            => value is null ? null : Newtonsoft.Json.JsonConvert.DeserializeObject<T>(System.Text.Encoding.UTF8.GetString(value));
    }
}
