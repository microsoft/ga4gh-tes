using System;
using LazyCache;
using Tes.Repository;

namespace TesApi.Web
{
    /// <summary>
    /// A wrapper for LazyCache.IAppCache
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class TesRepositoryLazyCache<T> : ICache<T> where T : class
    {
        /// <summary>
        /// A TesTask can run for 7 days, and hypothetically there could be weeks of queued tasks, so set a long default
        /// </summary>
        /// 
        private static TimeSpan defaultItemExpiration = TimeSpan.FromDays(30);

        private readonly IAppCache cache;

        /// <summary>
        /// Default constructor expecting the singleton LazyCache.IAppCache
        /// </summary>
        /// <param name="appCache"></param>
        public TesRepositoryLazyCache(IAppCache appCache)
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
            cache.Add(key, task, defaultItemExpiration);
            return true;
        }

        /// <inheritdoc/>
        public bool TryGetValue(string key, out T task)
        {
            return cache.TryGetValue(key, out task);
        }

        /// <inheritdoc/>
        public bool TryRemove(string key)
        {
            cache.Remove(key);
            return true;
        }

        /// <inheritdoc/>
        public bool TryUpdate(string key, T task)
        {
            return TryAdd(key, task);
        }
    }
}
