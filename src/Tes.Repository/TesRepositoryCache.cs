// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using Microsoft.Extensions.Caching.Memory;

namespace Tes.Repository
{
    /// <summary>
    /// A wrapper for IMemoryCache
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class TesRepositoryCache<T> : ICache<T> where T : class
    {
        /// <summary>
        /// A TesTask can run for 7 days, and hypothetically there could be weeks of queued tasks, so set a long default
        /// </summary>
        private static readonly TimeSpan defaultItemExpiration = TimeSpan.FromDays(30);

        private readonly IMemoryCache cache;

        private static object Key(string key)
        {
            return $"{nameof(TesRepositoryCache<T>)}:{key}";
        }

        /// <summary>
        /// Default constructor expecting the singleton LazyCache.IAppCache
        /// </summary>
        /// <param name="appCache"></param>
        public TesRepositoryCache(IMemoryCache appCache)
        {
            ArgumentNullException.ThrowIfNull(appCache);
            cache = appCache;
        }

        /// <inheritdoc/>
        public int MaxCount { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        /// <inheritdoc/>
        public int Count() => throw new NotSupportedException();

        /// <inheritdoc/>
        public bool TryAdd(string key, T task)
        {
            cache.Set(Key(key), task, defaultItemExpiration);
            return true;
        }

        /// <inheritdoc/>
        public bool TryGetValue(string key, out T task)
        {
            return cache.TryGetValue(Key(key), out task);
        }

        /// <inheritdoc/>
        public bool TryRemove(string key)
        {
            cache.Remove(Key(key));
            return true;
        }

        /// <inheritdoc/>
        public bool TryUpdate(string key, T task, System.TimeSpan expiration)
        {
            if (expiration == default)
            {
                expiration = defaultItemExpiration;
            }

            cache.Set(Key(key), task, expiration);
            return true;
        }
    }
}
