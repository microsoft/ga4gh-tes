// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Concurrent;
using System.Text.Json.Serialization;
using Azure.Core;
using Azure.Identity;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;
using Tes.Runner.Exceptions;
using Tes.Runner.Models;

namespace Tes.Runner.Authentication
{
    public partial class CredentialsManager
    {
        private readonly ILogger logger;
        private readonly RetryPolicy retryPolicy;
        private readonly ConcurrentDictionary<byte[], TokenCredential> cachedCredentals = new();
        private readonly ConcurrentDictionary<byte[], object> inflightLocks = new();

        private const int MaxRetryCount = 7;
        private const int ExponentialBackOffExponent = 2;

        public CredentialsManager(ILogger<CredentialsManager> logger)
        {
            ArgumentNullException.ThrowIfNull(logger);

            this.logger = logger;
            retryPolicy = Policy
                    .Handle<Exception>()
                    .WaitAndRetry(MaxRetryCount,
                    SleepDurationHandler);
        }

        /// <summary>
        /// Parameter-less constructor for mocking
        /// </summary>
        protected CredentialsManager() : this(Microsoft.Extensions.Logging.Abstractions.NullLogger<CredentialsManager>.Instance)
        { }

        private TimeSpan SleepDurationHandler(int attempt)
        {
            logger.LogInformation("Attempt {Attempt} to get token credential", attempt);
            var duration = TimeSpan.FromSeconds(Math.Pow(ExponentialBackOffExponent, attempt));
            logger.LogInformation("Waiting {Duration} before retrying", duration);
            return duration;
        }

        public virtual TokenCredential GetTokenCredential(RuntimeOptions runtimeOptions, string? tokenScope = default)
        {
            return GetTokenCredential(runtimeOptions, runtimeOptions.NodeManagedIdentityResourceId, tokenScope);
        }

        public virtual TokenCredential GetAcrPullTokenCredential(RuntimeOptions runtimeOptions, string? tokenScope = default)
        {
            var managedIdentity = runtimeOptions.NodeManagedIdentityResourceId;

            if (!string.IsNullOrWhiteSpace(runtimeOptions.AcrPullManagedIdentityResourceId))
            {
                managedIdentity = runtimeOptions.AcrPullManagedIdentityResourceId;
            }

            return GetTokenCredential(runtimeOptions, managedIdentity, tokenScope);
        }

        public virtual TokenCredential GetTokenCredential(RuntimeOptions runtimeOptions, string? managedIdentityResourceId, string? tokenScope = default)
        {
            tokenScope ??= runtimeOptions.AzureEnvironmentConfig!.TokenScope!;
            var key = MakeKey(new(runtimeOptions, tokenScope));

            lock (inflightLocks.GetOrAdd(key, _ => new()))
            {
                if (cachedCredentals.TryGetValue(key, out var tokenCredential))
                {
                    return tokenCredential;
                }

                try
                {
                    Uri azureAuthorityHost = new(runtimeOptions.AzureEnvironmentConfig!.AzureAuthorityHostUrl!);
                    return retryPolicy.Execute(() => GetTokenCredentialImpl(key, managedIdentityResourceId, tokenScope, azureAuthorityHost));
                }
                catch
                {
                    throw new IdentityUnavailableException();
                }
            }
        }

        private TokenCredential GetTokenCredentialImpl(byte[] key, string? managedIdentityResourceId, string tokenScope, Uri azureAuthorityHost)
        {
            try
            {
                TokenCredential tokenCredential;

                if (!string.IsNullOrWhiteSpace(managedIdentityResourceId))
                {
                    logger.LogInformation("Token credentials with Managed Identity and resource ID: {NodeManagedIdentityResourceId}", managedIdentityResourceId);
                    var tokenCredentialOptions = new TokenCredentialOptions { AuthorityHost = azureAuthorityHost };

                    tokenCredential = new ManagedIdentityCredential(
                        new ResourceIdentifier(managedIdentityResourceId),
                        tokenCredentialOptions);
                }
                else
                {
                    logger.LogInformation("Token credentials with DefaultAzureCredential");
                    var defaultAzureCredentialOptions = new DefaultAzureCredentialOptions { AuthorityHost = azureAuthorityHost };
                    tokenCredential = new DefaultAzureCredential(defaultAzureCredentialOptions);
                }

                //Get token to verify that credentials are valid
                _ = tokenCredential.GetToken(new TokenRequestContext([tokenScope]), CancellationToken.None);

                return cachedCredentals.AddOrUpdate(key, tokenCredential, (_, _) => tokenCredential);
            }
            catch (Exception e)
            {
                logger.LogError(e, "Failed to get token credential");
                throw;
            }
        }

        private static byte[] MakeKey(CredentialsManagerKeyType key)
        {
            return System.Text.Encoding.UTF8.GetBytes(System.Text.Json.JsonSerializer.Serialize(key, KeyTypeContext.Default.CredentialsManagerKeyType));
        }

        private record struct CredentialsManagerKeyType(RuntimeOptions runtimeOptions, string tokenScope);

        [JsonSerializable(typeof(CredentialsManagerKeyType))]
        [JsonSourceGenerationOptions(DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingDefault)]
        private partial class KeyTypeContext : JsonSerializerContext { }
    }
}
