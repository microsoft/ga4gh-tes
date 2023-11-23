// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Moq;
using Polly.Retry;
using Polly;
using static Tes.ApiClients.CachingRetryHandler;
using static Tes.ApiClients.RetryHandler;

namespace Tes.ApiClients.Tests.TestServices
{
    internal static partial class RetryHandlersHelpers
    {
        internal static Mock<ICachingAsyncPolicy<HttpResponseMessage>> GetCachingHttpResponseMessageAsyncRetryPolicyMock(Mock<CachingRetryHandler> cachingRetryHandler)
        {
            var cachingAsyncRetryPolicy = new Mock<ICachingAsyncPolicy<HttpResponseMessage>>();
            _ = cachingAsyncRetryPolicy.As<IRetryPolicy>();
            var cachingAsyncPolicy = cachingAsyncRetryPolicy.As<ICachingAsyncPolicy<HttpResponseMessage>>();
            var cachingPolicy = cachingAsyncPolicy.As<ICachingPolicy>();
            _ = cachingAsyncRetryPolicy.As<IAsyncPolicy<HttpResponseMessage>>();
            var cachingPolicyBuild = new Mock<ICachingPolicyBuilderBuild<HttpResponseMessage>>();
            cachingPolicyBuild.Setup(policy => policy.BuildAsync())
                .Returns(cachingAsyncRetryPolicy.Object);
            cachingRetryHandler.As<ICachingPolicyBuilderHandler>().Setup(policy => policy.CachingPolicyBuilder(It.IsAny<IPolicyBuilderBuild<HttpResponseMessage>>()))
                .Returns(cachingPolicyBuild.Object);
            var builderBuild = new Mock<IPolicyBuilderBuild<HttpResponseMessage>>();
            var policyBuilderWait = new Mock<IPolicyBuilderWait<HttpResponseMessage>>();
            policyBuilderWait.Setup(policy => policy.SetOnRetryBehavior(It.IsAny<Microsoft.Extensions.Logging.ILogger>(), It.IsAny<OnRetryHandler<HttpResponseMessage>>(), It.IsAny<OnRetryHandlerAsync<HttpResponseMessage>>()))
                .Returns(builderBuild.Object);
            cachingRetryHandler.Setup(cachingRetryHandler => cachingRetryHandler.RetryDefaultHttpResponseMessagePolicyBuilder())
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
