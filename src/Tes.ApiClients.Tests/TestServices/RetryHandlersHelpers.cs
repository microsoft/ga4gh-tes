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
        // TODO: Add ability to use a mocked ILogger with a mocked CachingRetryHandler where failures in the mocked retry handlers call the mocked ILogger.
        //    The opt-in would be an optional argument like this: "Microsoft.Extensions.Logging.ILogger logger".

        internal static Mock<ICachingAsyncPolicy> GetCachingAsyncRetryPolicyMock(Mock<CachingRetryHandler> cachingRetryHandler)
        {
            var cachingAsyncRetryPolicy = new Mock<ICachingAsyncPolicy>();
            _ = cachingAsyncRetryPolicy.As<IRetryPolicy>();
            var cachingAsyncPolicy = cachingAsyncRetryPolicy.As<ICachingAsyncPolicy>();
            var cachingPolicy = cachingAsyncPolicy.As<ICachingPolicy>();
            _ = new Mock<IAsyncPolicy>();
            var cachingPolicyBuild = new Mock<ICachingPolicyBuilderBuild>();
            cachingPolicyBuild.Setup(policy => policy.BuildAsync())
                .Returns(cachingAsyncRetryPolicy.Object);
            cachingRetryHandler.As<ICachingPolicyBuilderHandler>().Setup(policy => policy.CachingPolicyBuilder(It.IsAny<IPolicyBuilderBuild>()))
                .Returns(cachingPolicyBuild.Object);
            var builderBuild = new Mock<IPolicyBuilderBuild>();
            var policyBuilderWait = new Mock<IPolicyBuilderWait>();
            policyBuilderWait.Setup(policy => policy.SetOnRetryBehavior(It.IsAny<Microsoft.Extensions.Logging.ILogger>(), It.IsAny<OnRetryHandler>(), It.IsAny<OnRetryHandlerAsync>()))
                .Returns(builderBuild.Object);
            cachingRetryHandler.Setup(cachingRetryHandler => cachingRetryHandler.RetryDefaultPolicyBuilder())
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
