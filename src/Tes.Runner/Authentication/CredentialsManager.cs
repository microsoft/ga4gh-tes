// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using Azure.Identity;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;
using Tes.Runner.Exceptions;
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

        private const string DefaultTokenScope = @"https://management.azure.com/.default";

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

        public virtual TokenCredential GetTokenCredential(RuntimeOptions runtimeOptions)
        {
            try
            {
                return retryPolicy.Execute(() => GetTokenCredentialImpl(runtimeOptions));
            }
            catch
            {
                throw new IdentityUnavailableException();
            }
        }

        private TokenCredential GetTokenCredentialImpl(RuntimeOptions runtimeOptions)
        {
            try
            {
                TokenCredential tokenCredential;

                if (!string.IsNullOrWhiteSpace(runtimeOptions.NodeManagedIdentityResourceId))
                {
                    logger.LogInformation($"Token credentials with Managed Identity and resource ID: {runtimeOptions.NodeManagedIdentityResourceId}");

                    tokenCredential = new ManagedIdentityCredential(new ResourceIdentifier(runtimeOptions.NodeManagedIdentityResourceId));
                }
                else
                {
                    logger.LogInformation("Token credentials with DefaultAzureCredential");

                    tokenCredential = new DefaultAzureCredential();
                }

                //Get token to verify that credentials are valid
                tokenCredential.GetToken(new TokenRequestContext(new[] { DefaultTokenScope }), CancellationToken.None);

                return tokenCredential;
            }
            catch (Exception e)
            {
                logger.LogError(e, "Failed to get token credential");
                throw;
            }
        }
    }
}
