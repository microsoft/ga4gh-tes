// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq.Expressions;
using Moq;
using Polly;
using static CommonUtilities.RetryHandler;
using static CommonUtilities.RetryPolicyBuilder;
using static Tes.ApiClients.CachingRetryPolicyBuilder;

namespace Tes.ApiClients.Tests.TestServices
{
    internal static partial class RetryHandlersHelpers
    {
        // TODO: Add ability to use a mocked ILogger with a mocked CachingRetryHandler where failures in the mocked retry handlers call the mocked ILogger.
        //    The opt-in would be an optional argument like this: "Microsoft.Extensions.Logging.ILogger logger".

        internal static Mock<CachingRetryHandler.CachingAsyncRetryHandlerPolicy> GetCachingAsyncRetryPolicyMock(Mock<CachingRetryPolicyBuilder> cachingRetryHandler)
        {
            var cachingAsyncRetryPolicy = new Mock<CachingRetryHandler.CachingAsyncRetryHandlerPolicy>();
            _ = cachingAsyncRetryPolicy.As<IAsyncPolicy>();
            var cachingPolicyBuild = new Mock<ICachingPolicyBuilderBuild>();
            cachingPolicyBuild.Setup(policy => policy.AsyncBuild())
                .Returns(cachingAsyncRetryPolicy.Object);
            cachingRetryHandler.As<ICachingPolicyBuilderHandler>().Setup(policy => policy.CachingPolicyBuilder(It.IsAny<IPolicyBuilderBuild>()))
                .Returns(cachingPolicyBuild.Object);

            var builderBuild = new Mock<IPolicyBuilderBuild>();
            builderBuild.Setup(policy => policy.PolicyBuilderBase)
                .Returns(cachingRetryHandler.Object);
            builderBuild.Setup(c => c.AsyncBuildPolicy())
                .Returns((IAsyncPolicy)cachingAsyncRetryPolicy.Object);
            var builderWait = new Mock<IPolicyBuilderWait>();
            builderWait.Setup(c => c.SetOnRetryBehavior(It.IsAny<Microsoft.Extensions.Logging.ILogger>(), It.IsAny<OnRetryHandler>(), It.IsAny<OnRetryHandlerAsync>()))
                .Returns(builderBuild.Object);

            cachingRetryHandler.Setup(c => c.DefaultRetryPolicyBuilder())
                .Returns(builderWait.Object);

            cachingAsyncRetryPolicy.Setup(c => c.AppCache)
                .Returns(cachingRetryHandler.Object.AppCache);

            return cachingAsyncRetryPolicy;
        }

        internal static Mock<CachingRetryHandler.CachingAsyncRetryHandlerPolicy<TResult>> GetCachingAsyncRetryPolicyMock<TResult>(Mock<CachingRetryPolicyBuilder> cachingRetryHandler, Expression<Func<CachingRetryPolicyBuilder, IPolicyBuilderWait<TResult>>> expression)
        {
            var cachingAsyncRetryPolicy = new Mock<CachingRetryHandler.CachingAsyncRetryHandlerPolicy<TResult>>();
            _ = cachingAsyncRetryPolicy.As<IAsyncPolicy<TResult>>();
            var cachingPolicyBuild = new Mock<ICachingPolicyBuilderBuild<TResult>>();
            cachingPolicyBuild.Setup(policy => policy.AsyncBuild())
                .Returns(cachingAsyncRetryPolicy.Object);
            cachingRetryHandler.As<ICachingPolicyBuilderHandler>().Setup(policy => policy.CachingPolicyBuilder(It.IsAny<IPolicyBuilderBuild<TResult>>()))
                .Returns(cachingPolicyBuild.Object);

            var builderBuild = new Mock<IPolicyBuilderBuild<TResult>>();
            builderBuild.Setup(policy => policy.PolicyBuilderBase)
                .Returns(cachingRetryHandler.Object);
            builderBuild.Setup(c => c.AsyncBuildPolicy())
                .Returns((IAsyncPolicy<TResult>)cachingAsyncRetryPolicy.Object);
            var builderWait = new Mock<IPolicyBuilderWait<TResult>>();
            builderWait.Setup(c => c.SetOnRetryBehavior(It.IsAny<Microsoft.Extensions.Logging.ILogger>(), It.IsAny<OnRetryHandler<TResult>>(), It.IsAny<OnRetryHandlerAsync<TResult>>()))
                .Returns(builderBuild.Object);

            cachingRetryHandler.Setup(expression)
                .Returns(builderWait.Object);

            cachingAsyncRetryPolicy.Setup(c => c.AppCache)
                .Returns(cachingRetryHandler.Object.AppCache);

            return cachingAsyncRetryPolicy;
        }
    }
}
