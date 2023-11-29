// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Linq.Expressions;
using Moq;
using Polly;
using Polly.Retry;
using static Tes.ApiClients.CachingRetryHandler;
using static Tes.ApiClients.RetryHandler;

namespace Tes.ApiClients.Tests.TestServices
{
    internal static partial class RetryHandlersHelpers
    {
        // TODO: Add ability to use a mocked ILogger with a mocked CachingRetryHandler where failures in the mocked retry handlers call the mocked ILogger.
        //    The opt-in would be an optional argument like this: "Microsoft.Extensions.Logging.ILogger logger".

        internal static Mock<ICachingAsyncPolicy<TResult>> GetCachingAsyncRetryPolicyMock<TResult>(Mock<CachingRetryHandler> cachingRetryHandler, Expression<Func<CachingRetryHandler, IPolicyBuilderWait<TResult>>> retryDefaultTResultPolicyBuilder)
        {
            var cachingAsyncRetryPolicy = new Mock<ICachingAsyncPolicy<TResult>>();
            _ = cachingAsyncRetryPolicy.As<IRetryPolicy>();
            var cachingAsyncPolicy = cachingAsyncRetryPolicy.As<ICachingAsyncPolicy<TResult>>();
            var cachingPolicy = cachingAsyncPolicy.As<ICachingPolicy>();
            _ = cachingAsyncRetryPolicy.As<IAsyncPolicy<TResult>>();
            var cachingPolicyBuild = new Mock<ICachingPolicyBuilderBuild<TResult>>();
            cachingPolicyBuild.Setup(policy => policy.BuildAsync())
                .Returns(cachingAsyncRetryPolicy.Object);
            cachingRetryHandler.As<ICachingPolicyBuilderHandler>().Setup(policy => policy.CachingPolicyBuilder(It.IsAny<IPolicyBuilderBuild<TResult>>()))
                .Returns(cachingPolicyBuild.Object);
            var builderBuild = new Mock<IPolicyBuilderBuild<TResult>>();
            var policyBuilderWait = new Mock<IPolicyBuilderWait<TResult>>();
            policyBuilderWait.Setup(policy => policy.SetOnRetryBehavior(It.IsAny<Microsoft.Extensions.Logging.ILogger>(), It.IsAny<OnRetryHandler<TResult>>(), It.IsAny<OnRetryHandlerAsync<TResult>>()))
                .Returns(builderBuild.Object);
            cachingRetryHandler.Setup(retryDefaultTResultPolicyBuilder)
                .Returns(policyBuilderWait.Object);
            builderBuild.Setup(policy => policy.PolicyBuilderBase)
                .Returns(cachingRetryHandler.Object);
            cachingPolicy.Setup(c => c.Handler)
                .Returns(cachingRetryHandler.Object);
            cachingPolicy.Setup(c => c.AppCache)
                .Returns(cachingRetryHandler.Object.AppCache);
            return cachingAsyncRetryPolicy;
        }
    }
}
