// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using LazyCache;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Extensions.Http;
using Polly.Retry;
using TesApi.Web.Management.Configuration;

namespace TesApi.Web.Management
{
    /// <summary>
    /// Contains an App Cache instances and retry policies. 
    /// </summary>
    public class CacheAndRetryHandler
    {
        private readonly IAppCache appCache;
        private readonly AsyncRetryPolicy asyncRetryPolicy;
        private readonly AsyncRetryPolicy<HttpResponseMessage> asyncHttpRetryPolicy;

        /// <summary>
        /// Contains an App Cache instances and retry policies. 
        /// </summary>
        /// <param name="appCache"><see cref="IAppCache"/>></param>
        /// <param name="retryPolicyOptions"><see cref="RetryPolicyOptions"/></param>
        public CacheAndRetryHandler(IAppCache appCache, IOptions<RetryPolicyOptions> retryPolicyOptions)
        {
            ArgumentNullException.ThrowIfNull(appCache);
            ArgumentNullException.ThrowIfNull(retryPolicyOptions);

            this.appCache = appCache;
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
        /// Executes a delegate with the specified async policy. 
        /// </summary>
        /// <param name="action">Action to execute</param>
        /// <typeparam name="TResult">Result type</typeparam>
        /// <returns>Result instance</returns>
        public virtual Task<TResult> ExecuteWithRetryAsync<TResult>(Func<Task<TResult>> action)
        {
            ArgumentNullException.ThrowIfNull(action);

            return asyncRetryPolicy.ExecuteAsync(action);
        }

        /// <summary>
        /// Executes a delegate with the specified async policy. 
        /// </summary>
        /// <param name="action">Action to execute</param>
        /// <returns>Result instance</returns>
        public async Task ExecuteWithRetryAsync(Func<Task> action)
        {
            ArgumentNullException.ThrowIfNull(action);

            await asyncRetryPolicy.ExecuteAsync(action);
        }

        /// <summary>
        /// Executes a delegate with the specified async policy. 
        /// </summary>
        /// <param name="action">Action to execute</param>
        /// <returns>Result HttpResponse</returns>
        public virtual async Task<HttpResponseMessage> ExecuteHttpRequestWithRetryAsync(Func<Task<HttpResponseMessage>> action)
        {
            ArgumentNullException.ThrowIfNull(action);

            return await asyncHttpRetryPolicy.ExecuteAsync(action);
        }

        /// <summary>
        /// Executes a delegate with the specified async retry policy and persisting the result in a cache. 
        /// </summary>
        /// <param name="cacheKey"></param>
        /// <param name="action"></param>
        /// <returns></returns>
        public virtual async Task<TResult> ExecuteWithRetryAndCachingAsync<TResult>(string cacheKey, Func<Task<TResult>> action)
        {
            ValidateArgs(cacheKey, action);

            return await ExecuteWithCacheAsync(cacheKey, () => ExecuteWithRetryAsync(action));
        }

        /// <summary>
        /// Executes a delegate with the specified async retry policy and persisting the result in a cache, if the response is successful. 
        /// </summary>
        /// <param name="cacheKey"></param>
        /// <param name="action"></param>
        /// <returns></returns>
        public virtual async Task<HttpResponseMessage> ExecuteHttpRequestWithRetryAndCachingAsync(string cacheKey, Func<Task<HttpResponseMessage>> action)
        {
            ValidateArgs(cacheKey, action);

            if (appCache.TryGetValue(cacheKey, out HttpResponseMessage response))
            {
                return response;
            }

            response = await ExecuteHttpRequestWithRetryAsync(action);

            response.EnsureSuccessStatusCode();

            appCache.Add(cacheKey, response);

            return response;
        }

        private static void ValidateArgs(string cacheKey, Func<Task> action)
        {
            ArgumentNullException.ThrowIfNull(action);

            if (string.IsNullOrEmpty(cacheKey))
            {
                throw new ArgumentNullException(nameof(cacheKey), "Invalid cache key. The value can't be null or empty");
            }
        }

        private async Task<TResult> ExecuteWithCacheAsync<TResult>(string cacheKey, Func<Task<TResult>> action)
            => await appCache.GetOrAddAsync(cacheKey, action);
    }
}
