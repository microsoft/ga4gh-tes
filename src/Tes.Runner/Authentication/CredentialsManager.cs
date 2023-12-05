// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using Azure.Identity;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;
using Tes.Runner.Models;
using Tes.Runner.Transfer;

namespace Tes.Runner.Authentication
{
    public class CredentialsManager
    {
        private readonly ILogger logger = PipelineLoggerFactory.Create<CredentialsManager>();

        private readonly RetryPolicy retryPolicy;
        private const int MaxRetryCount = 7;
        private const int ExponentialBackOffExponent = 2;

        public CredentialsManager()
        {
            retryPolicy = Policy
                    .Handle<Exception>()
                    .WaitAndRetry(MaxRetryCount,
                    SleepDurationHandler);
        }

        private TimeSpan SleepDurationHandler(int attempt)
        {
            logger.LogInformation($"Attempt {attempt} to get token credential");
            var duration = TimeSpan.FromSeconds(Math.Pow(ExponentialBackOffExponent, attempt));
            logger.LogInformation($"Waiting {duration} before retrying");
            return duration;
        }

        public TokenCredential GetTokenCredential(RuntimeOptions runtimeOptions)
        {
            try
            {
                return retryPolicy.Execute(() => GetTokenCredentialImpl(runtimeOptions));
            }
            catch (CredentialUnavailableException)
            {
                throw new IdentityUnavailableException();
            }
        }
        private TokenCredential GetTokenCredentialImpl(RuntimeOptions runtimeOptions)
        {
            try
            {
                if (!string.IsNullOrWhiteSpace(runtimeOptions.NodeManagedIdentityResourceId))
                {
                    logger.LogInformation($"Token credentials with Managed Identity and resource ID: {runtimeOptions.NodeManagedIdentityResourceId}");

                    return new ManagedIdentityCredential(new ResourceIdentifier(runtimeOptions.NodeManagedIdentityResourceId));
                }

                logger.LogInformation("Token credentials with DefaultAzureCredential");

                return new DefaultAzureCredential();
            }
            catch (Exception e)
            {
                logger.LogError(e, "Failed to get token credential");
                throw;
            }
        }
    }
}
