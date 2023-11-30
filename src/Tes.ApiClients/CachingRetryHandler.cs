// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using Tes.ApiClients.Options;

namespace Tes.ApiClients
{
    /// <summary>
    /// Contains an App Cache instances and retry policies.
    /// </summary>
    public partial class CachingRetryHandler : RetryHandler, CachingRetryHandler.ICachingPolicyBuilderHandler
    {
        private readonly IMemoryCache appCache = null!;
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

        #region CachingRetryHandlerPolicies

        public class CachingRetryHandlerPolicy : RetryHandlerPolicy, ICachingPolicy
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

        public class CachingAsyncRetryHandlerPolicy : AsyncRetryHandlerPolicy, ICachingPolicy
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

                return await ExecuteWithCacheAsync(appCache, cacheKey, () => ExecuteWithRetryAsync(action, cancellationToken), cachesExpires);
            }
        }

        //public class CachingRetryHandlerPolicy<TResult> : RetryHandlerPolicy<TResult>, ICachingPolicy
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

        public class CachingAsyncRetryHandlerPolicy<TResult> : AsyncRetryHandlerPolicy<TResult>, ICachingPolicy
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
            ///  Executes a delegate with the specified async retry policy and persisting the result in a cache.
            /// </summary>
            /// <param name="cacheKey"></param>
            /// <param name="action">Action to execute</param>
            /// <param name="cachesExpires"></param>
            /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
            /// <returns></returns>
            public virtual async Task<TResult> ExecuteWithRetryAndCachingAsync(string cacheKey, Func<CancellationToken, Task<TResult>> action, DateTimeOffset cachesExpires, CancellationToken cancellationToken)
            {
                ValidateArgs(cacheKey, action);

                return await ExecuteWithCacheAsync(appCache, cacheKey, () => ExecuteWithRetryAsync(action, cancellationToken), cachesExpires);
            }

            /// <summary>
            /// Executes a delegate with the specified async retry policy and persisting the result in a cache.
            /// </summary>
            /// <typeparam name="T">Final return type</typeparam>
            /// <param name="cacheKey"></param>
            /// <param name="action">Action to execute</param>
            /// <param name="convert">Method to convert</param>
            /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
            /// <param name="caller">Name of method originating the retriable operation.</param>
            /// <returns></returns>
            public virtual async Task<T> ExecuteWithRetryAndCachingAsync<T>(string cacheKey, Func<CancellationToken, Task<TResult>> action, Func<TResult, CancellationToken, Task<T>> convert, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string caller = default)
            {
                ValidateArgs(cacheKey, action);

                return await ExecuteWithCacheAsync(appCache, cacheKey, async () => await convert(await ExecuteWithRetryAsync(action, cancellationToken, caller), cancellationToken));
            }

            /// <summary>
            ///  Executes a delegate with the specified async retry policy and persisting the result in a cache.
            /// </summary>
            /// <typeparam name="T">Final return type</typeparam>
            /// <param name="cacheKey"></param>
            /// <param name="action">Action to execute</param>
            /// <param name="convert">Method to convert</param>
            /// <param name="cachesExpires"></param>
            /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
            /// <param name="caller">Name of method originating the retriable operation.</param>
            /// <returns></returns>
            public virtual async Task<T> ExecuteWithRetryAndCachingAsync<T>(string cacheKey, Func<CancellationToken, Task<TResult>> action, Func<TResult, CancellationToken, Task<T>> convert, DateTimeOffset cachesExpires, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string caller = default)
            {
                ValidateArgs(cacheKey, action);

                return await ExecuteWithCacheAsync(appCache, cacheKey, async () => await convert(await ExecuteWithRetryAsync(action, cancellationToken, caller), cancellationToken), cachesExpires);
            }
        }
        #endregion

        #region Builder interfaces
        public interface ICachingPolicy
        {
            IMemoryCache AppCache { get; }
        }

        public interface ICachingPolicyBuilderBuild
        {
            /// <summary>
            /// Builds <see cref="RetryPolicy"/> with caching.
            /// </summary>
            /// <returns>Caching retry policy.</returns>
            CachingRetryHandlerPolicy Build();

            /// <summary>
            /// Builds <see cref="AsyncRetryPolicy"/> with caching.
            /// </summary>
            /// <returns>Caching retry policy.</returns>
            CachingAsyncRetryHandlerPolicy BuildAsync();
        }

        public interface ICachingPolicyBuilderBuild<TResult>
        {
            ///// <summary>
            ///// Builds <see cref="RetryPolicy"/> with caching.
            ///// </summary>
            ///// <returns>Caching retry policy.</returns>
            //CachingRetryHandlerPolicy<TResult> Build();

            /// <summary>
            /// Builds <see cref="AsyncRetryPolicy"/> with caching.
            /// </summary>
            /// <returns>Caching retry policy.</returns>
            CachingAsyncRetryHandlerPolicy<TResult> BuildAsync();
        }

        /// <remarks>Used internally and for testing.</remarks>
        public interface ICachingPolicyBuilderHandler
        {
            /// <remarks>Used internally and for testing.</remarks>
            ICachingPolicyBuilderBuild CachingPolicyBuilder(IPolicyBuilderBuild policyBuilder);
            /// <remarks>Used internally and for testing.</remarks>
            ICachingPolicyBuilderBuild<TResult> CachingPolicyBuilder<TResult>(IPolicyBuilderBuild<TResult> policyBuilder);
        }
        #endregion

        #region Builder interface implementations
        ICachingPolicyBuilderBuild ICachingPolicyBuilderHandler.CachingPolicyBuilder(IPolicyBuilderBuild policyBuilder)
            => new CachingPolicyBuilderBuild(policyBuilder, this);

        ICachingPolicyBuilderBuild<TResult> ICachingPolicyBuilderHandler.CachingPolicyBuilder<TResult>(IPolicyBuilderBuild<TResult> policyBuilder)
            => new CachingPolicyBuilderBuild<TResult>(policyBuilder, this);

        private readonly struct CachingPolicyBuilderBuild : ICachingPolicyBuilderBuild
        {
            private readonly IPolicyBuilderBuild policyBuilder;
            private readonly CachingRetryHandler cachingHandler;

            public CachingPolicyBuilderBuild(IPolicyBuilderBuild policyBuilder, CachingRetryHandler handler)
            {
                ArgumentNullException.ThrowIfNull(policyBuilder);
                this.policyBuilder = policyBuilder;
                this.cachingHandler = handler;
            }

            CachingRetryHandlerPolicy ICachingPolicyBuilderBuild.Build()
            {
                return new(policyBuilder.BuildPolicy(), cachingHandler.AppCache);
            }

            CachingAsyncRetryHandlerPolicy ICachingPolicyBuilderBuild.BuildAsync()
            {
                return new(policyBuilder.BuildAsyncPolicy(), cachingHandler.AppCache);
            }
        }

        private readonly struct CachingPolicyBuilderBuild<TResult> : ICachingPolicyBuilderBuild<TResult>
        {
            private readonly IPolicyBuilderBuild<TResult> policyBuilder;
            private readonly CachingRetryHandler cachingHandler;

            public CachingPolicyBuilderBuild(IPolicyBuilderBuild<TResult> policyBuilder, CachingRetryHandler handler)
            {
                ArgumentNullException.ThrowIfNull(policyBuilder);
                this.policyBuilder = policyBuilder;
                this.cachingHandler = handler;
            }

            //CachingRetryHandlerPolicy<TResult> ICachingPolicyBuilderBuild<TResult>.Build()
            //{
            //    return new(policyBuilder.BuildPolicy(), cachingHandler.AppCache);
            //}

            CachingAsyncRetryHandlerPolicy<TResult> ICachingPolicyBuilderBuild<TResult>.BuildAsync()
            {
                return new(policyBuilder.BuildAsyncPolicy(), cachingHandler.AppCache);
            }
        }
        #endregion

        internal static void ValidateArgs(string cacheKey, Func<CancellationToken, Task> action)
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
    }

    /// <summary>
    /// Extension methods for <see cref="CachingRetryHandler"/>
    /// </summary>
    public static class CachingRetryHandlerExtensions
    {
        /// <summary>
        /// Default caching policy.
        /// </summary>
        /// <param name="policyBuilder"><see cref="RetryHandler"/> policy builder.</param>
        /// <returns>OnRetry builder</returns>
        public static CachingRetryHandler.ICachingPolicyBuilderBuild AddCaching(this RetryHandler.IPolicyBuilderBuild policyBuilder)
        {
            return ((CachingRetryHandler.ICachingPolicyBuilderHandler)policyBuilder.PolicyBuilderBase).CachingPolicyBuilder(policyBuilder);
        }

        /// <summary>
        /// Default caching policy.
        /// </summary>
        /// <param name="policyBuilder"><see cref="RetryHandler"/> policy builder.</param>
        /// <returns>OnRetry builder</returns>
        public static CachingRetryHandler.ICachingPolicyBuilderBuild<TResult> AddCaching<TResult>(this RetryHandler.IPolicyBuilderBuild<TResult> policyBuilder)
        {
            return ((CachingRetryHandler.ICachingPolicyBuilderHandler)policyBuilder.PolicyBuilderBase).CachingPolicyBuilder(policyBuilder);
        }
    }
}
