// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using CommonUtilities;
using CommonUtilities.Options;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Polly.Retry;

namespace Tes.ApiClients
{
    /// <summary>
    /// Contains an App Cache instances and retry policies.
    /// </summary>
    public class CachingRetryPolicyBuilder : RetryPolicyBuilder, CachingRetryPolicyBuilder.ICachingPolicyBuilderHandler
    {
        private readonly IMemoryCache appCache = null!;
        public virtual IMemoryCache AppCache => appCache;

        /// <summary>
        /// Contains an App Cache instances and retry policies.
        /// </summary>
        /// <param name="appCache"><see cref="IMemoryCache"/>></param>
        /// <param name="retryPolicyOptions"><see cref="RetryPolicyOptions"/></param>
        public CachingRetryPolicyBuilder(IMemoryCache appCache, IOptions<RetryPolicyOptions> retryPolicyOptions) : base(retryPolicyOptions)
        {
            ArgumentNullException.ThrowIfNull(appCache);

            this.appCache = appCache;
        }

        /// <summary>
        /// Protected parameter-less constructor for mocking
        /// </summary>
        protected CachingRetryPolicyBuilder() { }

        #region Builder interfaces
        public interface ICachingPolicyBuilderBuild
        {
            /// <summary>
            /// Builds <see cref="RetryPolicy"/> with caching.
            /// </summary>
            /// <returns>Caching retry policy.</returns>
            CachingRetryHandler.CachingRetryHandlerPolicy SyncBuild();

            /// <summary>
            /// Builds <see cref="AsyncRetryPolicy"/> with caching.
            /// </summary>
            /// <returns>Caching retry policy.</returns>
            CachingRetryHandler.CachingAsyncRetryHandlerPolicy AsyncBuild();
        }

        public interface ICachingPolicyBuilderBuild<TResult>
        {
            ///// <summary>
            ///// Builds <see cref="RetryPolicy"/> with caching.
            ///// </summary>
            ///// <returns>Caching retry policy.</returns>
            //CachingRetryHandler.CachingRetryHandlerPolicy<TResult> SyncBuild();

            /// <summary>
            /// Builds <see cref="AsyncRetryPolicy"/> with caching.
            /// </summary>
            /// <returns>Caching retry policy.</returns>
            CachingRetryHandler.CachingAsyncRetryHandlerPolicy<TResult> AsyncBuild();
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
            private readonly CachingRetryPolicyBuilder cachingHandler;

            public CachingPolicyBuilderBuild(IPolicyBuilderBuild policyBuilder, CachingRetryPolicyBuilder handler)
            {
                ArgumentNullException.ThrowIfNull(policyBuilder);
                this.policyBuilder = policyBuilder;
                this.cachingHandler = handler;
            }

            CachingRetryHandler.CachingRetryHandlerPolicy ICachingPolicyBuilderBuild.SyncBuild()
            {
                return new(policyBuilder.SyncBuildPolicy(), cachingHandler.AppCache);
            }

            CachingRetryHandler.CachingAsyncRetryHandlerPolicy ICachingPolicyBuilderBuild.AsyncBuild()
            {
                return new(policyBuilder.AsyncBuildPolicy(), cachingHandler.AppCache);
            }
        }

        private readonly struct CachingPolicyBuilderBuild<TResult> : ICachingPolicyBuilderBuild<TResult>
        {
            private readonly IPolicyBuilderBuild<TResult> policyBuilder;
            private readonly CachingRetryPolicyBuilder cachingHandler;

            public CachingPolicyBuilderBuild(IPolicyBuilderBuild<TResult> policyBuilder, CachingRetryPolicyBuilder handler)
            {
                ArgumentNullException.ThrowIfNull(policyBuilder);
                this.policyBuilder = policyBuilder;
                this.cachingHandler = handler;
            }

            //CachingRetryHandler.CachingRetryHandlerPolicy<TResult> ICachingPolicyBuilderBuild<TResult>.SyncBuild()
            //{
            //    return new(policyBuilder.SyncBuildPolicy(), cachingHandler.AppCache);
            //}

            CachingRetryHandler.CachingAsyncRetryHandlerPolicy<TResult> ICachingPolicyBuilderBuild<TResult>.AsyncBuild()
            {
                return new(policyBuilder.AsyncBuildPolicy(), cachingHandler.AppCache);
            }
        }
        #endregion
    }

    /// <summary>
    /// Extension methods for <see cref="CachingRetryPolicyBuilder"/>
    /// </summary>
    public static class CachingRetryHandlerExtensions
    {
        /// <summary>
        /// Default caching policy.
        /// </summary>
        /// <param name="policyBuilder"><see cref="RetryPolicyBuilder"/> policy builder.</param>
        /// <returns>OnRetry builder</returns>
        public static CachingRetryPolicyBuilder.ICachingPolicyBuilderBuild AddCaching(this RetryPolicyBuilder.IPolicyBuilderBuild policyBuilder)
        {
            return ((CachingRetryPolicyBuilder.ICachingPolicyBuilderHandler)policyBuilder.PolicyBuilderBase).CachingPolicyBuilder(policyBuilder);
        }

        /// <summary>
        /// Default caching policy.
        /// </summary>
        /// <param name="policyBuilder"><see cref="RetryPolicyBuilder"/> policy builder.</param>
        /// <returns>OnRetry builder</returns>
        public static CachingRetryPolicyBuilder.ICachingPolicyBuilderBuild<TResult> AddCaching<TResult>(this RetryPolicyBuilder.IPolicyBuilderBuild<TResult> policyBuilder)
        {
            return ((CachingRetryPolicyBuilder.ICachingPolicyBuilderHandler)policyBuilder.PolicyBuilderBase).CachingPolicyBuilder(policyBuilder);
        }
    }
}
