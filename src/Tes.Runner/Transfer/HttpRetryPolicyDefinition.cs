// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using CommonUtilities;
using Microsoft.Extensions.Logging;
using static CommonUtilities.RetryHandler;

namespace Tes.Runner.Transfer
{
    public class HttpRetryPolicyDefinition
    {
        public const int DefaultMaxRetryCount = 9;
        public const int RetryExponent = 2;
        private static readonly ILogger Logger = PipelineLoggerFactory.Create<HttpRetryPolicyDefinition>();

        public static AsyncRetryHandlerPolicy DefaultAsyncRetryPolicy(int maxRetryCount = DefaultMaxRetryCount)
        {
            return new RetryPolicyBuilder(Microsoft.Extensions.Options.Options.Create(new CommonUtilities.Options.RetryPolicyOptions() { MaxRetryCount = maxRetryCount, ExponentialBackOffExponent = RetryExponent }))
                .PolicyBuilder.OpinionatedRetryPolicy(Polly.Policy.Handle<RetriableException>())
                .WithRetryPolicyOptionsWait()
                .SetOnRetryBehavior(Logger)
                .AsyncBuild();
        }
    }
}
