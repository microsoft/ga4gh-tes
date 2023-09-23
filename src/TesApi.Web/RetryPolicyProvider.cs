// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Polly.Retry;
using Polly;
using System;

namespace TesApi.Web
{
    /// <summary>
    /// Utility class that facilitates the retry policy implementations
    /// </summary>
    public interface IRetryPolicyProvider
    {
        /// <summary>
        /// Creates a default retry policy for critical services that retries 12 times with a 5 second delay between each retry.
        /// </summary>
        /// <returns>An async retry policy</returns>
        AsyncRetryPolicy CreateDefaultCriticalServiceRetryPolicy();
    }

    /// <inheritdoc />
    public class RetryPolicyProvider : IRetryPolicyProvider
    {
        /// <inheritdoc />
        public AsyncRetryPolicy CreateDefaultCriticalServiceRetryPolicy()
        {
            return Policy.Handle<Exception>().WaitAndRetryAsync(12, attempt => TimeSpan.FromSeconds(5));
        }
    }
}
