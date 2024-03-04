// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Logging;
using Tes.ApiClients;
using static Tes.ApiClients.CachingRetryHandler;

namespace TesApi.Web
{
    /// <summary>
    /// Common base for caching retry handlers
    /// </summary>
    public abstract class CachingWithRetriesBase
    {
        /// <summary>
        /// Sync retry policy.
        /// </summary>
        protected readonly CachingRetryHandlerPolicy cachingRetry;
        /// <summary>
        /// Async retry policy.
        /// </summary>
        protected readonly CachingAsyncRetryHandlerPolicy cachingAsyncRetry;
        /// <summary>
        /// Async retry policy for methods where Exists should not be retried.
        /// </summary>
        protected readonly CachingAsyncRetryHandlerPolicy cachingAsyncRetryExceptWhenExists;
        /// <summary>
        /// Async retry policy for methods where NotFound should not be retried.
        /// </summary>
        protected readonly CachingAsyncRetryHandlerPolicy cachingAsyncRetryExceptWhenNotFound;
        /// <summary>
        /// Logger.
        /// </summary>
        protected readonly ILogger logger;

        /// <summary>
        /// Contructor to create a cache of <see cref="IAzureProxy"/>
        /// </summary>
        /// <param name="cachingRetryHandler"></param>
        /// <param name="logger"></param>
        public CachingWithRetriesBase(CachingRetryPolicyBuilder cachingRetryHandler, ILogger logger)
        {
            ArgumentNullException.ThrowIfNull(cachingRetryHandler);

            this.logger = logger;

            var sleepDuration = new Func<int, Exception, TimeSpan?>((attempt, exception) => (exception as BatchException)?.RequestInformation?.RetryAfter);

            this.cachingRetry = cachingRetryHandler.PolicyBuilder
                .OpinionatedRetryPolicy()
                .WithExceptionBasedWaitWithRetryPolicyOptionsBackup(sleepDuration, backupSkipProvidedIncrements: true).SetOnRetryBehavior(this.logger).AddCaching().SyncBuild();

            this.cachingAsyncRetry = cachingRetryHandler.PolicyBuilder
                .OpinionatedRetryPolicy()
                .WithExceptionBasedWaitWithRetryPolicyOptionsBackup(sleepDuration, backupSkipProvidedIncrements: true).SetOnRetryBehavior(this.logger).AddCaching().AsyncBuild();

            this.cachingAsyncRetryExceptWhenExists = cachingRetryHandler.PolicyBuilder
                .OpinionatedRetryPolicy(Polly.Policy.Handle<BatchException>(ex => !CreationErrorFoundCodes.Contains(ex.RequestInformation?.BatchError?.Code, StringComparer.OrdinalIgnoreCase)))
                .WithExceptionBasedWaitWithRetryPolicyOptionsBackup(sleepDuration, backupSkipProvidedIncrements: true).SetOnRetryBehavior(this.logger).AddCaching().AsyncBuild();

            this.cachingAsyncRetryExceptWhenNotFound = cachingRetryHandler.PolicyBuilder
                .OpinionatedRetryPolicy(Polly.Policy.Handle<BatchException>(ex => !DeletionErrorFoundCodes.Contains(ex.RequestInformation?.BatchError?.Code, StringComparer.OrdinalIgnoreCase)))
                .WithExceptionBasedWaitWithRetryPolicyOptionsBackup(sleepDuration, backupSkipProvidedIncrements: true).SetOnRetryBehavior(this.logger).AddCaching().AsyncBuild();
        }

        private static readonly string[] CreationErrorFoundCodes = new[]
        {
            BatchErrorCodeStrings.TaskExists,
            BatchErrorCodeStrings.PoolExists,
            BatchErrorCodeStrings.JobExists
        };

        private static readonly string[] DeletionErrorFoundCodes = new[]
        {
            BatchErrorCodeStrings.TaskNotFound,
            BatchErrorCodeStrings.PoolNotFound,
            BatchErrorCodeStrings.JobNotFound
        };
    }
}
