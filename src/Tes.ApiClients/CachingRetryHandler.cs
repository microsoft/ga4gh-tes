// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using CommonUtilities;
using Microsoft.Extensions.Caching.Memory;
using Polly;

namespace Tes.ApiClients
{
    /// <summary>
    /// Contains an App Cache instances and retry policies.
    /// </summary>
    public static class CachingRetryHandler
    {
        public interface ICachingPolicy
        {
            IMemoryCache AppCache { get; }
        }

        #region CachingRetryHandlerPolicies
        public class CachingRetryHandlerPolicy : RetryHandler.RetryHandlerPolicy, ICachingPolicy
        {
            private readonly IMemoryCache appCache;

            public CachingRetryHandlerPolicy(ISyncPolicy retryPolicy, IMemoryCache appCache)
                : base(retryPolicy)
                => this.appCache = appCache;

            /// <remarks>For mocking</remarks>
            public CachingRetryHandlerPolicy() { }


            /// <summary>
            /// App cache instance.
            /// </summary>
            public virtual IMemoryCache AppCache => appCache;

            /// <summary>
            /// Executes a delegate with the specified policy.
            /// </summary>
            /// <param name="retryPolicy">Synchronous retry policy.</param>
            /// <param name="action">Action to execute.</param>
            /// <param name="caller">Name of method originating the retriable operation.</param>
            /// <returns><typeparamref name="TResult"/> instance.</returns>
            public TResult ExecuteWithRetryAndCaching<TResult>(string cacheKey, Func<TResult> action, [System.Runtime.CompilerServices.CallerMemberName] string caller = default)
            {
                ArgumentNullException.ThrowIfNull(action);

                return appCache.GetOrCreate(cacheKey, _ => ExecuteWithRetry(action, caller));
            }
        }

        public class CachingAsyncRetryHandlerPolicy : RetryHandler.AsyncRetryHandlerPolicy, ICachingPolicy
        {
            private readonly IMemoryCache appCache;

            public CachingAsyncRetryHandlerPolicy(IAsyncPolicy retryPolicy, IMemoryCache appCache)
                : base(retryPolicy)
                => this.appCache = appCache;

            /// <remarks>For mocking</remarks>
            public CachingAsyncRetryHandlerPolicy() { }


            /// <summary>
            /// App cache instance.
            /// </summary>
            public virtual IMemoryCache AppCache => appCache;

            /// <summary>
            /// Executes a delegate with the specified async retry policy and persisting the result in a cache.
            /// </summary>
            /// <param name="cacheKey"></param>
            /// <param name="action">Action to execute</param>
            /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
            /// <param name="caller">Name of method originating the retriable operation.</param>
            /// <returns></returns>
            public virtual async Task<TResult> ExecuteWithRetryAndCachingAsync<TResult>(string cacheKey, Func<CancellationToken, Task<TResult>> action, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string caller = default)
            {
                ValidateArgs(cacheKey, action);

                return await ExecuteWithCacheAsync(appCache, cacheKey, () => ExecuteWithRetryAsync(action, cancellationToken, caller));
            }

            /// <summary>
            /// Executes a delegate with the specified async retry policy and persisting the result in a cache.
            /// </summary>
            /// <param name="cacheKey"></param>
            /// <param name="action">Action to execute</param>
            /// <param name="cachesExpires"></param>
            /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
            /// <param name="caller">Name of method originating the retriable operation.</param>
            /// <typeparam name="TResult"></typeparam>
            /// <returns></returns>
            public virtual async Task<TResult> ExecuteWithRetryAndCachingAsync<TResult>(string cacheKey, Func<CancellationToken, Task<TResult>> action, DateTimeOffset cachesExpires, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string caller = default)
            {
                ValidateArgs(cacheKey, action);

                return await ExecuteWithCacheAsync(appCache, cacheKey, () => ExecuteWithRetryAsync(action, cancellationToken, caller), cachesExpires);
            }

            /// <summary>
            /// Executes a delegate with the specified async retry policy and persisting the result in a cache.
            /// </summary>
            /// <param name="cacheKey"></param>
            /// <param name="action">Action to execute</param>
            /// <param name="cachesExpires"></param>
            /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
            /// <param name="caller">Name of method originating the retriable operation.</param>
            /// <typeparam name="TResult"></typeparam>
            /// <returns></returns>
            public virtual async Task<TResult> ExecuteWithRetryAndCachingAsync<TResult>(string cacheKey, Func<CancellationToken, Task<TResult>> action, Func<TResult, DateTimeOffset> cachesExpires, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string caller = default)
            {
                ValidateArgs(cacheKey, action);

                return await ExecuteWithCacheAsync(appCache, cacheKey, () => ExecuteWithRetryAsync(action, cancellationToken, caller), cachesExpires);
            }
        }

        //public class CachingRetryHandlerPolicy<TResult> : RetryHandler.RetryHandlerPolicy<TResult>, ICachingPolicy
        //{
        //    private readonly IMemoryCache appCache;

        //    public CachingRetryHandlerPolicy(ISyncPolicy<TResult> retryPolicy, IMemoryCache appCache)
        //        : base(retryPolicy)
        //        => this.appCache = appCache;

        //    /// <remarks>For mocking</remarks>
        //    public CachingRetryHandlerPolicy() { }


        //    /// <summary>
        //    /// App cache instance.
        //    /// </summary>
        //    public virtual IMemoryCache AppCache => appCache;
        //}

        public class CachingAsyncRetryHandlerPolicy<TResult> : RetryHandler.AsyncRetryHandlerPolicy<TResult>, ICachingPolicy
        {
            private readonly IMemoryCache appCache;

            public CachingAsyncRetryHandlerPolicy(IAsyncPolicy<TResult> retryPolicy, IMemoryCache appCache)
                : base(retryPolicy)
                => this.appCache = appCache;

            /// <remarks>For mocking</remarks>
            public CachingAsyncRetryHandlerPolicy() { }


            /// <summary>
            /// App cache instance.
            /// </summary>
            public virtual IMemoryCache AppCache => appCache;

            /// <summary>
            /// Executes a delegate with the specified async retry policy and persisting the result in a cache.
            /// </summary>
            /// <param name="cacheKey"></param>
            /// <param name="action">Action to execute</param>
            /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
            /// <param name="caller">Name of method originating the retriable operation.</param>
            /// <returns></returns>
            public virtual async Task<TResult> ExecuteWithRetryAndCachingAsync(string cacheKey, Func<CancellationToken, Task<TResult>> action, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string caller = default)
            {
                ValidateArgs(cacheKey, action);

                return await ExecuteWithCacheAsync(appCache, cacheKey, () => ExecuteWithRetryAsync(action, cancellationToken, caller));
            }

            /// <summary>
            /// Executes a delegate with the specified async retry policy and persisting the result in a cache.
            /// </summary>
            /// <param name="cacheKey"></param>
            /// <param name="action">Action to execute</param>
            /// <param name="cachesExpires"></param>
            /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
            /// <param name="caller">Name of method originating the retriable operation.</param>
            /// <returns></returns>
            public virtual async Task<TResult> ExecuteWithRetryAndCachingAsync(string cacheKey, Func<CancellationToken, Task<TResult>> action, DateTimeOffset cachesExpires, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string caller = default)
            {
                ValidateArgs(cacheKey, action);

                return await ExecuteWithCacheAsync(appCache, cacheKey, () => ExecuteWithRetryAsync(action, cancellationToken, caller), cachesExpires);
            }

            /// <summary>
            /// Executes a delegate with the specified async retry policy and persisting the result in a cache.
            /// </summary>
            /// <typeparam name="T">Instance type in cache</typeparam>
            /// <param name="cacheKey"></param>
            /// <param name="action">Action to execute</param>
            /// <param name="convert">Method to convert</param>
            /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
            /// <param name="caller">Name of method originating the retriable operation.</param>
            /// <returns></returns>
            public virtual async Task<T> ExecuteWithRetryConversionAndCachingAsync<T>(string cacheKey, Func<CancellationToken, Task<TResult>> action, Func<TResult, CancellationToken, Task<T>> convert, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string caller = default)
            {
                ValidateArgs(cacheKey, action);

                return await ExecuteWithCacheAsync(appCache, cacheKey, () => ExecuteWithRetryAndConversionAsync(action, convert, cancellationToken, caller));
            }

            /// <summary>
            /// Executes a delegate with the specified async retry policy and persisting the result in a cache.
            /// </summary>
            /// <typeparam name="T">Instance type in cache</typeparam>
            /// <param name="cacheKey"></param>
            /// <param name="action">Action to execute</param>
            /// <param name="convert">Method to convert</param>
            /// <param name="cachesExpires"></param>
            /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
            /// <param name="caller">Name of method originating the retriable operation.</param>
            /// <returns></returns>
            public virtual async Task<T> ExecuteWithRetryConversionAndCachingAsync<T>(string cacheKey, Func<CancellationToken, Task<TResult>> action, Func<TResult, CancellationToken, Task<T>> convert, DateTimeOffset cachesExpires, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string caller = default)
            {
                ValidateArgs(cacheKey, action);

                return await ExecuteWithCacheAsync(appCache, cacheKey, () => ExecuteWithRetryAndConversionAsync(action, convert, cancellationToken, caller), cachesExpires);
            }

            /// <summary>
            /// Executes a delegate with the specified async retry policy and persisting the result in a cache.
            /// </summary>
            /// <param name="cacheKey"></param>
            /// <param name="action">Action to execute</param>
            /// <param name="cachesExpires"></param>
            /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
            /// <param name="caller">Name of method originating the retriable operation.</param>
            /// <returns></returns>
            public virtual async Task<TResult> ExecuteWithRetryAndCachingAsync(string cacheKey, Func<CancellationToken, Task<TResult>> action, Func<TResult, DateTimeOffset> cachesExpires, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string caller = default)
            {
                ValidateArgs(cacheKey, action);

                return await ExecuteWithCacheAsync(appCache, cacheKey, () => ExecuteWithRetryAsync(action, cancellationToken, caller), cachesExpires);
            }

            /// <summary>
            /// Executes a delegate with the specified async retry policy and persisting the result in a cache.
            /// </summary>
            /// <typeparam name="T">Instance type in cache</typeparam>
            /// <param name="cacheKey"></param>
            /// <param name="action">Action to execute</param>
            /// <param name="convert">Method to convert</param>
            /// <param name="cachesExpires"></param>
            /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
            /// <param name="caller">Name of method originating the retriable operation.</param>
            /// <returns></returns>
            public virtual async Task<T> ExecuteWithRetryConversionAndCachingAsync<Result, T>(string cacheKey, Func<CancellationToken, Task<TResult>> action, Func<TResult, CancellationToken, Task<T>> convert, Func<T, DateTimeOffset> cachesExpires, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string caller = default)
            {
                ValidateArgs(cacheKey, action);

                return await ExecuteWithCacheAsync(appCache, cacheKey, () => ExecuteWithRetryAndConversionAsync(action, convert, cancellationToken, caller), cachesExpires);
            }
        }
        #endregion

        private static void ValidateArgs(string cacheKey, Func<CancellationToken, Task> action)
        {
            ArgumentNullException.ThrowIfNull(action);

            if (string.IsNullOrEmpty(cacheKey))
            {
                throw new ArgumentNullException(nameof(cacheKey), "Invalid cache key. The value can't be null or empty");
            }
        }

        private static async Task<TResult> ExecuteWithCacheAsync<TResult>(IMemoryCache appCache, string cacheKey, Func<Task<TResult>> action)
            => await appCache.GetOrCreateAsync(cacheKey, _ => action());

        private static async Task<TResult> ExecuteWithCacheAsync<TResult>(IMemoryCache appCache, string cacheKey, Func<Task<TResult>> action, DateTimeOffset cacheExpires)
            => await appCache.GetOrCreateAsync(cacheKey, entry =>
            {
                entry.AbsoluteExpiration = cacheExpires;
                return action();
            });

        private static async Task<TResult> ExecuteWithCacheAsync<TResult>(IMemoryCache appCache, string cacheKey, Func<Task<TResult>> action, Func<TResult, DateTimeOffset> cacheExpires)
            => await appCache.GetOrCreateAsync(cacheKey, async entry =>
            {
                var result = await action();
                entry.AbsoluteExpiration = cacheExpires(result);
                return result;
            });
    }
}
