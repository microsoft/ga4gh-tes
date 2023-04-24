﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using LazyCache;

namespace Tes.Repository
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
        private static readonly System.TimeSpan defaultItemExpiration = System.TimeSpan.FromDays(30);

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
            cache.Add($"{nameof(TesRepositoryLazyCache<T>)}:{key}", task, defaultItemExpiration);
            return true;
        }

        /// <inheritdoc/>
        public bool TryGetValue(string key, out T task)
        {
            return cache.TryGetValue($"{nameof(TesRepositoryLazyCache<T>)}:{key}", out task);
        }

        /// <inheritdoc/>
        public bool TryRemove(string key)
        {
            cache.Remove($"{nameof(TesRepositoryLazyCache<T>)}:{key}");
            return true;
        }

        /// <inheritdoc/>
        public bool TryUpdate(string key, T task)
        {
            return TryAdd($"{nameof(TesRepositoryLazyCache<T>)}:{key}", task);
        }
    }
}
