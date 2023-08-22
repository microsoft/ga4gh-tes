// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Net;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Extensions.Http;
using Polly.Retry;
using Tes.ApiClients.Options;

namespace Tes.ApiClients
{
    /// <summary>
    /// Contains an App Cache instances and retry policies. 
    /// </summary>
    public class CacheAndRetryHandler
    {
        private readonly IMemoryCache appCache = null!;
        private readonly RetryPolicy retryPolicy = null!;
        private readonly AsyncRetryPolicy asyncRetryPolicy = null!;
        private readonly AsyncRetryPolicy<HttpResponseMessage> asyncHttpRetryPolicy = null!;

        /// <summary>
        /// Synchronous retry policy instance.
        /// </summary>
        public virtual RetryPolicy RetryPolicy => retryPolicy;
        /// <summary>
        /// Asynchronous retry policy instance.
        /// </summary>
        public virtual AsyncRetryPolicy AsyncRetryPolicy => asyncRetryPolicy;
        /// <summary>
        /// App cache instance.
        /// </summary>
        public virtual IMemoryCache AppCache => appCache;

        /// <summary>
        /// Contains an App Cache instances and retry policies. 
        /// </summary>
        /// <param name="appCache"><see cref="IMemoryCache"/>></param>
        /// <param name="retryPolicyOptions"><see cref="RetryPolicyOptions"/></param>
        public CacheAndRetryHandler(IMemoryCache appCache, IOptions<RetryPolicyOptions> retryPolicyOptions)
        {
            ArgumentNullException.ThrowIfNull(appCache);
            ArgumentNullException.ThrowIfNull(retryPolicyOptions);

            this.appCache = appCache;
            this.retryPolicy = Policy
                .Handle<Exception>()
                .WaitAndRetry(retryPolicyOptions.Value.MaxRetryCount,
                    (attempt) => TimeSpan.FromSeconds(Math.Pow(retryPolicyOptions.Value.ExponentialBackOffExponent,
                        attempt)));
            this.asyncRetryPolicy = Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(retryPolicyOptions.Value.MaxRetryCount,
                    (attempt) => TimeSpan.FromSeconds(Math.Pow(retryPolicyOptions.Value.ExponentialBackOffExponent,
                        attempt)));
            this.asyncHttpRetryPolicy = HttpPolicyExtensions.HandleTransientHttpError()
                .OrResult(r => r.StatusCode == HttpStatusCode.TooManyRequests)
                .WaitAndRetryAsync(retryPolicyOptions.Value.MaxRetryCount,
                    (attempt) => TimeSpan.FromSeconds(Math.Pow(retryPolicyOptions.Value.ExponentialBackOffExponent,
                        attempt)));
        }

        /// <summary>
        /// Protected parameterless constructor
        /// </summary>
        protected CacheAndRetryHandler() { }


        /// <summary>
        /// Executes a delegate with the specified policy.
        /// </summary>
        /// <param name="action">Action to execute</param>
        /// <returns>Result instance</returns>
        public void ExecuteWithRetry(Action action)
        {
            ArgumentNullException.ThrowIfNull(action);

            retryPolicy.Execute(action);
        }

        /// <summary>
        /// Executes a delegate with the specified policy.
        /// </summary>
        /// <param name="action">Action to execute</param>
        /// <returns>Result instance</returns>
        public TResult ExecuteWithRetry<TResult>(Func<TResult> action)
        {
            ArgumentNullException.ThrowIfNull(action);

            return retryPolicy.Execute(action);
        }

        /// <summary>
        /// Executes a delegate with the specified async policy.
        /// </summary>
        /// <param name="action">Action to execute</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <typeparam name="TResult">Result type</typeparam>
        /// <returns>Result instance</returns>
        public virtual Task<TResult> ExecuteWithRetryAsync<TResult>(Func<CancellationToken, Task<TResult>> action, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(action);

            return asyncRetryPolicy.ExecuteAsync(ct => action(ct), cancellationToken);
        }

        /// <summary>
        /// Executes a delegate with the specified async policy.
        /// </summary>
        /// <param name="action">Action to execute</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>Result instance</returns>
        public async Task ExecuteWithRetryAsync(Func<CancellationToken, Task> action, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(action);

            await asyncRetryPolicy.ExecuteAsync(ct => action(ct), cancellationToken);
        }

        /// <summary>
        /// Executes a delegate with the specified async policy. 
        /// </summary>
        /// <param name="action">Action to execute</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>Result HttpResponse</returns>
        public virtual async Task<HttpResponseMessage> ExecuteHttpRequestWithRetryAsync(Func<CancellationToken, Task<HttpResponseMessage>> action, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(action);

            return await asyncHttpRetryPolicy.ExecuteAsync(ct => action(ct), cancellationToken);
        }

        /// <summary>
        /// Executes a delegate with the specified async retry policy and persisting the result in a cache.
        /// </summary>
        /// <param name="cacheKey"></param>
        /// <param name="action">Action to execute</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public virtual async Task<TResult> ExecuteWithRetryAndCachingAsync<TResult>(string cacheKey, Func<CancellationToken, Task<TResult>> action, CancellationToken cancellationToken)
        {
            ValidateArgs(cacheKey, action);

            return await ExecuteWithCacheAsync(cacheKey, () => ExecuteWithRetryAsync(action, cancellationToken));
        }

        /// <summary>
        ///  Executes a delegate with the specified async retry policy and persisting the result in a cache.
        /// </summary>
        /// <param name="cacheKey"></param>
        /// <param name="action">Action to execute</param>
        /// <param name="cachesExpires"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <typeparam name="TResult"></typeparam>
        /// <returns></returns>
        public virtual async Task<TResult> ExecuteWithRetryAndCachingAsync<TResult>(string cacheKey, Func<CancellationToken, Task<TResult>> action, DateTimeOffset cachesExpires, CancellationToken cancellationToken)
        {
            ValidateArgs(cacheKey, action);

            return await ExecuteWithCacheAsync(cacheKey, () => ExecuteWithRetryAsync(action, cancellationToken), cachesExpires);
        }

        /// <summary>
        /// Executes a delegate with the specified async retry policy and persisting the result in a cache, if the response is successful. 
        /// </summary>
        /// <param name="cacheKey"></param>
        /// <param name="action"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public virtual async Task<HttpResponseMessage> ExecuteHttpRequestWithRetryAndCachingAsync(string cacheKey, Func<CancellationToken, Task<HttpResponseMessage>> action, CancellationToken cancellationToken)
        {
            ValidateArgs(cacheKey, action);

            if (appCache.TryGetValue(cacheKey, out HttpResponseMessage? response))
            {
                if (response is null)
                {
                    throw new InvalidOperationException("The value found in the cache is null");
                }
                return response!;
            }

            response = await ExecuteHttpRequestWithRetryAsync(action, cancellationToken);

            response.EnsureSuccessStatusCode();

            appCache.Set(cacheKey, response);

            return response;
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
            => (await appCache.GetOrCreateAsync(cacheKey, _1 => action()))!;

        private async Task<TResult> ExecuteWithCacheAsync<TResult>(string cacheKey, Func<Task<TResult>> action, DateTimeOffset cacheExpires)
            => (await appCache.GetOrCreateAsync(cacheKey, entry =>
            {
                entry.AbsoluteExpiration = cacheExpires;
                return action();
            }))!;
    }
}
