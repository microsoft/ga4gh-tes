// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Logging;
using Polly;
using Polly.Contrib.WaitAndRetry;
using Polly.Retry;

namespace Tes.Runner.Transfer
{
    public class HttpRetryPolicyDefinition
    {
        public const int DefaultMaxRetryCount = 14;
        private static readonly ILogger Logger = PipelineLoggerFactory.Create<HttpRetryPolicyDefinition>();

        public static AsyncRetryPolicy DefaultAsyncRetryPolicy(int maxRetryCount = DefaultMaxRetryCount)
        {
            return Policy
                .Handle<RetriableException>()
                .WaitAndRetryAsync(
                    sleepDurations: Backoff.DecorrelatedJitterBackoffV2(
                            medianFirstRetryDelay: TimeSpan.FromSeconds(1),
                            retryCount: maxRetryCount)
                        .Select(s => TimeSpan.FromTicks(Math.Max(s.Ticks, TimeSpan.FromMinutes(9).Ticks))),
                    onRetryAsync: (exception, _, retryCount, _) =>
                    {
                        Logger.LogError(exception, "Retrying failed request. Retry count: {retryCount}", retryCount);
                        return Task.CompletedTask;
                    });
        }
    }
}
