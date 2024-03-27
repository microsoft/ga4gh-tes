// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using CommonUtilities;
using CommonUtilities.Options;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Auth;
using TokenCredential = Azure.Core.TokenCredential;

namespace Tes.ApiClients
{
    /// <summary>
    /// Base client for Terra API clients
    /// </summary>
    public abstract class TerraApiClient : HttpApiClient
    {
        protected readonly string ApiUrl = null!;

        /// <summary>
        /// Protected parameter-less constructor
        /// </summary>
        protected TerraApiClient() { }

        /// <summary>
        /// Protected constructor of TerraApiClient
        /// </summary>
        /// <param name="apiUrl">API Host</param>
        /// <param name="tokenCredential"><see cref="TokenCredential"/></param>
        /// <param name="cachingRetryPolicyBuilder"><see cref="CachingRetryPolicyBuilder"/></param>
        /// <param name="logger"><see cref="ILogger{TCategoryName}"/></param>
        protected TerraApiClient(string apiUrl, TokenCredential tokenCredential, CachingRetryPolicyBuilder cachingRetryPolicyBuilder, AzureEnvironmentConfig azureCloudIdentityConfig, ILogger logger) : base(tokenCredential, azureCloudIdentityConfig.TokenScope, cachingRetryPolicyBuilder, logger)
        {
            ArgumentException.ThrowIfNullOrEmpty(apiUrl);
            ArgumentNullException.ThrowIfNull(tokenCredential);
            ArgumentNullException.ThrowIfNull(cachingRetryPolicyBuilder);
            ArgumentNullException.ThrowIfNull(azureCloudIdentityConfig);
            ArgumentNullException.ThrowIfNull(logger);

            ApiUrl = apiUrl;
        }

        protected static T CreateTerraApiClient<T>(string apiUrl, IMemoryCache sharedMemoryCache, TokenCredential tokenCredential, AzureEnvironmentConfig azureCloudIdentityConfig) where T : TerraApiClient {
            var retryPolicyOptions = new RetryPolicyOptions();
            var cacheRetryHandler = new CachingRetryPolicyBuilder(sharedMemoryCache, Microsoft.Extensions.Options.Options.Create(retryPolicyOptions));

            return (T)Activator.CreateInstance(typeof(T), 
                apiUrl, 
                tokenCredential, 
                cacheRetryHandler, 
                azureCloudIdentityConfig, 
                ApiClientsLoggerFactory.Create<T>());
        }
    }
}
