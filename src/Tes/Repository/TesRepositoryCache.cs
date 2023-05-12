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
        private readonly DistributedCacheEntryOptions defaultItemOptions = new() { SlidingExpiration = System.TimeSpan.FromDays(1) };
        private readonly IDistributedCache cache;

        static TesRepositoryCache() => Utilities.NewtonsoftJsonSafeInit.SetDefaultSettings();

        /// <summary>
        /// Default constructor expecting the singleton IDistributedCache
        /// </summary>
        /// <param name="appCache"></param>
        public TesRepositoryCache(IDistributedCache appCache)
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
            cache.Set(Key(key), Convert(task), defaultItemOptions);
            return true;
        }

        /// <inheritdoc/>
        public bool TryGetValue(string key, out T task)
        {
            task = Convert(cache.Get(Key(key)));
            return task is not null;
        }

        /// <inheritdoc/>
        public bool TryRemove(string key)
        {
            var cacheKey = Key(key);

            if (cache.Get(cacheKey) is null)
            {
                return false;
            }

            cache.Remove(cacheKey);
            return true;
        }

        /// <inheritdoc/>
        public bool TryUpdate(string key, T task)
        {
            cache.Remove(Key(key));
            return TryAdd(key, task);
        }

        private static byte[] Convert(T value)
        {
            return value is null
                ? null
                : System.Text.Encoding.UTF8.GetBytes(Newtonsoft.Json.JsonConvert.SerializeObject(value));
        }

        private static T Convert(byte[] value)
        {
            return value is null
                ? null
                : Newtonsoft.Json.JsonConvert.DeserializeObject<T>(System.Text.Encoding.UTF8.GetString(value));
        }
    }
}
