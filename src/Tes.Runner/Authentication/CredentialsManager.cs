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

        public CredentialsManager()
        {
            retryPolicy = Policy
                    .Handle<Exception>()
                    .WaitAndRetry(MaxRetryCount,
                    SleepDurationHandler);
        }

        private TimeSpan SleepDurationHandler(int attempt)
        {
            logger.LogInformation("Attempt {Attempt} to get token credential", attempt);
            var duration = TimeSpan.FromSeconds(Math.Pow(ExponentialBackOffExponent, attempt));
            logger.LogInformation("Waiting {Duration} before retrying", duration);
            return duration;
        }

        public virtual TokenCredential GetTokenCredential(RuntimeOptions runtimeOptions, string? tokenScope = default)
        {
            return GetTokenCredential(runtimeOptions, r => r.NodeManagedIdentityResourceId, tokenScope);
        }

        public virtual TokenCredential GetTokenCredential(RuntimeOptions runtimeOptions, Func<RuntimeOptions, string?> getManagedIdentityResourceId, string? tokenScope = default)
        {
            try
            {
                return retryPolicy.Execute(() => GetTokenCredentialImpl(runtimeOptions, getManagedIdentityResourceId, tokenScope));
            }
            catch
            {
                throw new IdentityUnavailableException();
            }
        }

        private TokenCredential GetTokenCredentialImpl(RuntimeOptions runtimeOptions, Func<RuntimeOptions, string?> getManagedIdentityResourceId, string? tokenScope)
        {
            tokenScope ??= runtimeOptions.AzureEnvironmentConfig!.TokenScope!;
            var managedIdentityResourceId = getManagedIdentityResourceId(runtimeOptions);

            try
            {
                TokenCredential tokenCredential;
                Uri authorityHost = new(runtimeOptions.AzureEnvironmentConfig!.AzureAuthorityHostUrl!);

                if (!string.IsNullOrWhiteSpace(managedIdentityResourceId))
                {
                    logger.LogInformation("Token credentials with Managed Identity and resource ID: {NodeManagedIdentityResourceId}", managedIdentityResourceId);
                    var tokenCredentialOptions = new TokenCredentialOptions { AuthorityHost = authorityHost };

                    tokenCredential = new ManagedIdentityCredential(
                        new ResourceIdentifier(managedIdentityResourceId),
                        tokenCredentialOptions);
                }
                else
                {
                    logger.LogInformation("Token credentials with DefaultAzureCredential");
                    var defaultAzureCredentialOptions = new DefaultAzureCredentialOptions { AuthorityHost = authorityHost };
                    tokenCredential = new DefaultAzureCredential(defaultAzureCredentialOptions);
                }

                //Get token to verify that credentials are valid
                _ = tokenCredential.GetToken(new TokenRequestContext([tokenScope]), CancellationToken.None);

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
