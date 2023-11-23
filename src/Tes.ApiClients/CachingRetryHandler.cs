// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using Tes.ApiClients.Options;
using static Tes.ApiClients.CachingRetryHandler;

namespace Tes.ApiClients
{
    /// <summary>
    /// Extension methods for <see cref="CachingRetryHandler"/>
    /// </summary>
    public static class CachingRetryHandlerExtensions
    {
        /// <summary>
        /// Executes a delegate with the specified async retry policy and persisting the result in a cache.
        /// </summary>
        /// <param name="retryPolicy">Asynchronous caching retry policy</param>
        /// <param name="cacheKey"></param>
        /// <param name="action">Action to execute</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="caller">Name of method originating the retriable operation.</param>
        /// <returns></returns>
        public static Task<TResult> ExecuteWithRetryAndCachingAsync<TResult>(this ICachingAsyncPolicy retryPolicy, string cacheKey, Func<CancellationToken, Task<TResult>> action, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string caller = default)
        {
            ArgumentNullException.ThrowIfNull(retryPolicy);
            ValidateArgs(cacheKey, action);

            return retryPolicy.Handler.ExecuteWithCacheAsync(cacheKey, () => retryPolicy.ExecuteWithRetryAsync(action, cancellationToken, caller));
        }

        /// <summary>
        ///  Executes a delegate with the specified async retry policy and persisting the result in a cache.
        /// </summary>
        /// <param name="retryPolicy">Asynchronous caching retry policy</param>
        /// <param name="cacheKey"></param>
        /// <param name="action">Action to execute</param>
        /// <param name="cachesExpires"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="caller">Name of method originating the retriable operation.</param>
        /// <typeparam name="TResult"></typeparam>
        /// <returns></returns>
        public static Task<TResult> ExecuteWithRetryAndCachingAsync<TResult>(this ICachingAsyncPolicy retryPolicy, string cacheKey, Func<CancellationToken, Task<TResult>> action, DateTimeOffset cachesExpires, CancellationToken cancellationToken, [System.Runtime.CompilerServices.CallerMemberName] string caller = default)
        {
            ArgumentNullException.ThrowIfNull(retryPolicy);
            ValidateArgs(cacheKey, action);

            return retryPolicy.Handler.ExecuteWithCacheAsync(cacheKey, () => retryPolicy.ExecuteWithRetryAsync(action, cancellationToken, caller), cachesExpires);
        }

        public static ICachingPolicyBuilderBuild AddCaching(this RetryHandler.IPolicyBuilderBuild policyBuilder)
        {
            return ((ICachingPolicyBuilderHandler)policyBuilder.PolicyBuilderBase).CachingPolicyBuilder(policyBuilder);
        }

        public static ICachingPolicyBuilderBuild<TResult> AddCaching<TResult>(this RetryHandler.IPolicyBuilderBuild<TResult> policyBuilder)
        {
            return ((ICachingPolicyBuilderHandler)policyBuilder.PolicyBuilderBase).CachingPolicyBuilder(policyBuilder);
        }
    }


    /// <summary>
    /// Contains an App Cache instances and retry policies. 
    /// </summary>
    public partial class CachingRetryHandler : RetryHandler, ICachingPolicyBuilderHandler
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

        #region Builder interfaces
        public interface ICachingPolicy
        {
            IMemoryCache AppCache { get; }

            /// <remarks>Used internally and for testing.</remarks>
            public CachingRetryHandler Handler { get; }
        }

        public interface ICachingSyncPolicy : ICachingPolicy, ISyncPolicy { }
        public interface ICachingSyncPolicy<TResult> : ICachingPolicy, ISyncPolicy<TResult> { }
        public interface ICachingAsyncPolicy : ICachingPolicy, IAsyncPolicy { }
        public interface ICachingAsyncPolicy<TResult> : ICachingPolicy, IAsyncPolicy<TResult> { }

        public interface ICachingPolicyBuilderBuild
        {
            ICachingSyncPolicy Build();
            ICachingAsyncPolicy BuildAsync();
        }

        public interface ICachingPolicyBuilderBuild<TResult>
        {
            ICachingSyncPolicy<TResult> Build();
            ICachingAsyncPolicy<TResult> BuildAsync();
        }

        /// <remarks>Used internally and for testing.</remarks>
        public interface ICachingPolicyBuilderHandler
        {
            ICachingPolicyBuilderBuild CachingPolicyBuilder(IPolicyBuilderBuild policyBuilder);
            ICachingPolicyBuilderBuild<TResult> CachingPolicyBuilder<TResult>(IPolicyBuilderBuild<TResult> policyBuilder);
        }
        #endregion

        #region Builder interface implementations
        ICachingPolicyBuilderBuild ICachingPolicyBuilderHandler.CachingPolicyBuilder(IPolicyBuilderBuild policyBuilder)
            => new CachingPolicyBuilderBuild<IPolicyBuilderBuild>(policyBuilder, this);

        ICachingPolicyBuilderBuild<TResult> ICachingPolicyBuilderHandler.CachingPolicyBuilder<TResult>(IPolicyBuilderBuild<TResult> policyBuilder)
            => new CachingPolicyBuilderBuild<TResult>(policyBuilder, this);

        private readonly struct CachingPolicyBuilderBuild<TResult> : ICachingPolicyBuilderBuild, ICachingPolicyBuilderBuild<TResult>
        {
            private readonly IPolicyBuilderBuild policyBuilder;
            private readonly IPolicyBuilderBuild<TResult> genericPolicyBuilder;
            private readonly CachingRetryHandler cachingHandler;

            public CachingPolicyBuilderBuild(IPolicyBuilderBuild policyBuilder, CachingRetryHandler handler)
            {
                ArgumentNullException.ThrowIfNull(policyBuilder);
                this.policyBuilder = policyBuilder;
                this.cachingHandler = handler;
            }

            public CachingPolicyBuilderBuild(IPolicyBuilderBuild<TResult> policyBuilder, CachingRetryHandler handler)
            {
                ArgumentNullException.ThrowIfNull(policyBuilder);
                this.genericPolicyBuilder = policyBuilder;
                this.cachingHandler = handler;
            }

            public ICachingSyncPolicy Build()
            {
                return new CachingRetryPolicy(cachingHandler, policyBuilder.Build());
            }

            public ICachingAsyncPolicy BuildAsync()
            {
                return new CachingAsyncRetryPolicy(cachingHandler, policyBuilder.BuildAsync());
            }

            ICachingSyncPolicy<TResult> ICachingPolicyBuilderBuild<TResult>.Build()
            {
                return new CachingRetryPolicy<TResult>(cachingHandler, genericPolicyBuilder.Build());
            }

            ICachingAsyncPolicy<TResult> ICachingPolicyBuilderBuild<TResult>.BuildAsync()
            {
                return new CachingAsyncRetryPolicy<TResult>(cachingHandler, genericPolicyBuilder.BuildAsync());
            }
        }

        private partial class CachingRetryPolicy : IRetryPolicy, ICachingSyncPolicy
        {
            [BeaKona.AutoInterface(typeof(ISyncPolicy), IncludeBaseInterfaces = true)]
            [BeaKona.AutoInterface(typeof(IRetryPolicy), IncludeBaseInterfaces = true)]
            private readonly ISyncPolicy policy;
            private readonly CachingRetryHandler handler;

            public IMemoryCache AppCache => handler.AppCache;
            CachingRetryHandler ICachingPolicy.Handler => handler;

            public CachingRetryPolicy(CachingRetryHandler handler, ISyncPolicy policy)
            {
                ArgumentNullException.ThrowIfNull(policy);
                this.policy = policy;
                this.handler = handler;
            }
        }

        private partial class CachingRetryPolicy<TResult> : IRetryPolicy<TResult>, ICachingSyncPolicy<TResult>
        {
            [BeaKona.AutoInterface(IncludeBaseInterfaces = true)]
            private readonly ISyncPolicy<TResult> policy;
            [BeaKona.AutoInterface(IncludeBaseInterfaces = true)]
            private readonly IRetryPolicy<TResult> retryPolicy;
            private readonly CachingRetryHandler handler;

            public IMemoryCache AppCache => handler.AppCache;
            CachingRetryHandler ICachingPolicy.Handler => handler;

            public CachingRetryPolicy(CachingRetryHandler handler, ISyncPolicy<TResult> policy)
            {
                ArgumentNullException.ThrowIfNull(policy);
                retryPolicy = (IRetryPolicy<TResult>)policy;
                this.policy = policy;
                this.handler = handler;
            }
        }

        private partial class CachingAsyncRetryPolicy : IRetryPolicy, ICachingAsyncPolicy
        {
            [BeaKona.AutoInterface(typeof(IAsyncPolicy), IncludeBaseInterfaces = true)]
            [BeaKona.AutoInterface(typeof(IRetryPolicy), IncludeBaseInterfaces = true)]
            private readonly IAsyncPolicy policy;
            private readonly CachingRetryHandler handler;

            public IMemoryCache AppCache => handler.AppCache;
            CachingRetryHandler ICachingPolicy.Handler => handler;

            public CachingAsyncRetryPolicy(CachingRetryHandler handler, IAsyncPolicy policy)
            {
                ArgumentNullException.ThrowIfNull(policy);
                this.policy = policy;
                this.handler = handler;
            }
        }

        private partial class CachingAsyncRetryPolicy<TResult> : IRetryPolicy<TResult>, ICachingAsyncPolicy<TResult>
        {
            [BeaKona.AutoInterface(IncludeBaseInterfaces = true)]
            private readonly IAsyncPolicy<TResult> policy;
            [BeaKona.AutoInterface(IncludeBaseInterfaces = true)]
            private readonly IRetryPolicy<TResult> retryPolicy;
            private readonly CachingRetryHandler handler;

            public IMemoryCache AppCache => handler.AppCache;
            CachingRetryHandler ICachingPolicy.Handler => handler;

            public CachingAsyncRetryPolicy(CachingRetryHandler handler, IAsyncPolicy<TResult> policy)
            {
                retryPolicy = (IRetryPolicy<TResult>)policy;
                this.policy = policy;
                this.handler = handler;
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

        internal async Task<TResult> ExecuteWithCacheAsync<TResult>(string cacheKey, Func<Task<TResult>> action)
            => await appCache.GetOrCreateAsync(cacheKey, _ => action());

        internal async Task<TResult> ExecuteWithCacheAsync<TResult>(string cacheKey, Func<Task<TResult>> action, DateTimeOffset cacheExpires)
            => await appCache.GetOrCreateAsync(cacheKey, entry =>
            {
                entry.AbsoluteExpiration = cacheExpires;
                return action();
            });
    }
}
