// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Polly;
using Tes.ApiClients.Options;

namespace Tes.ApiClients
{
    /// <summary>
    /// Contains an App Cache instances and retry policies. 
    /// </summary>
    public class CachingRetryHandler : RetryHandler
    {
        private readonly IMemoryCache appCache = null!;

        /// <summary>
        /// App cache instance.
        /// </summary>
        public virtual IMemoryCache AppCache => appCache;

        /// <summary>
        /// Contains an App Cache instances and retry policies. 
        /// </summary>
        /// <param name="appCache"><see cref="IMemoryCache"/>></param>
        /// <param name="retryPolicyOptions"><see cref="RetryPolicyOptions"/></param>
        public CachingRetryHandler(IMemoryCache appCache, IOptions<RetryPolicyOptions> retryPolicyOptions) : base(retryPolicyOptions)
        {
            ArgumentNullException.ThrowIfNull(appCache);

            this.appCache = appCache;
        }

        /// <summary>
        /// Protected parameter-less constructor for mocking
        /// </summary>
        protected CachingRetryHandler() { }


        /// <summary>
        /// Executes a delegate with the specified async retry policy and persisting the result in a cache.
        /// </summary>
        /// <param name="cacheKey"></param>
        /// <param name="action">Action to execute</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="context"></param>
        /// <returns></returns>
        public virtual async Task<TResult> ExecuteWithRetryAndCachingAsync<TResult>(string cacheKey, Func<CancellationToken, Task<TResult>> action, CancellationToken cancellationToken, Context context = default)
        {
            ValidateArgs(cacheKey, action);

            return await ExecuteWithCacheAsync(cacheKey, () => ExecuteWithRetryAsync(action, cancellationToken, context));
        }

        /// <summary>
        ///  Executes a delegate with the specified async retry policy and persisting the result in a cache.
        /// </summary>
        /// <param name="cacheKey"></param>
        /// <param name="action">Action to execute</param>
        /// <param name="cachesExpires"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="context"></param>
        /// <typeparam name="TResult"></typeparam>
        /// <returns></returns>
        public virtual async Task<TResult> ExecuteWithRetryAndCachingAsync<TResult>(string cacheKey, Func<CancellationToken, Task<TResult>> action, DateTimeOffset cachesExpires, CancellationToken cancellationToken, Context context = default)
        {
            ValidateArgs(cacheKey, action);

            return await ExecuteWithCacheAsync(cacheKey, () => ExecuteWithRetryAsync(action, cancellationToken, context), cachesExpires);
        }

        private static void ValidateArgs(string cacheKey, Func<CancellationToken, Task> action)
        {
            ArgumentNullException.ThrowIfNull(action);

            if (string.IsNullOrEmpty(cacheKey))
            {
                throw new ArgumentNullException(nameof(cacheKey), "Invalid cache key. The value can't be null or empty");
            }
        }

        private async Task<TResult> ExecuteWithCacheAsync<TResult>(string cacheKey, Func<Task<TResult>> action)
            => (await appCache.GetOrCreateAsync(cacheKey, _ => action()))!;

        private async Task<TResult> ExecuteWithCacheAsync<TResult>(string cacheKey, Func<Task<TResult>> action, DateTimeOffset cacheExpires)
            => (await appCache.GetOrCreateAsync(cacheKey, entry =>
            {
                entry.AbsoluteExpiration = cacheExpires;
                return action();
            }))!;
    }
}
