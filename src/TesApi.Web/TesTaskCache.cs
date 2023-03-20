using System;
using LazyCache;
using Tes.Repository;

namespace TesApi.Web
{
    public class TesRepositoryLazyCache<T> : ICache<T> where T : class
    {
        /// <summary>
        /// A TesTask can run for 7 days, and hypothetically there could be weeks of queued tasks, so set a long default
        /// </summary>
        /// 
        private static TimeSpan defaultItemExpiration = TimeSpan.FromDays(30);

        private IAppCache cache;

        public TesRepositoryLazyCache(IAppCache appCache)
        {
            cache = appCache;
        }

        public int MaxCount { get => throw new System.NotSupportedException(); set => throw new System.NotSupportedException(); }

        public int Count()
        {
            throw new System.NotSupportedException();
        }

        public bool TryAdd(string key, T task)
        {
            cache.Add(key, task, defaultItemExpiration);
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
