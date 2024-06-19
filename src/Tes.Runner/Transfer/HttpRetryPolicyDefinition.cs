// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;

namespace Tes.Runner.Transfer
{
    public class HttpRetryPolicyDefinition
    {
        public const int DefaultMaxRetryCount = 9;
        public const int RetryExponent = 2;

        public static AsyncRetryPolicy DefaultAsyncRetryPolicy(ILogger logger, int maxRetryCount = DefaultMaxRetryCount)
        {
            return Policy
                .Handle<RetriableException>()
                .WaitAndRetryAsync(maxRetryCount, retryAttempt =>
                    {
                        return TimeSpan.FromSeconds(Math.Pow(RetryExponent, retryAttempt));
                    },
                    onRetryAsync:
                    (exception, _, retryCount, _) =>
                    {
                        logger.LogError(exception, "Retrying failed request. Retry count: {retryCount}", retryCount);
                        return Task.CompletedTask;
                    });
        }
    }
}
