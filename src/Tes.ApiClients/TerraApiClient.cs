// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using CommonUtilities;
using Microsoft.Extensions.Logging;

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
        /// <param name="cachingRetryHandler"><see cref="CachingRetryPolicyBuilder"/></param>
        /// <param name="logger"><see cref="ILogger{TCategoryName}"/></param>
        protected TerraApiClient(string apiUrl, TokenCredential tokenCredential, CachingRetryPolicyBuilder cachingRetryHandler, AzureEnvironmentConfig azureCloudIdentityConfig, ILogger logger) : base(tokenCredential, azureCloudIdentityConfig.TokenScope, cachingRetryHandler, logger)
        {
            ArgumentException.ThrowIfNullOrEmpty(apiUrl);
            ArgumentNullException.ThrowIfNull(tokenCredential);
            ArgumentNullException.ThrowIfNull(cachingRetryHandler);
            ArgumentNullException.ThrowIfNull(azureCloudIdentityConfig);
            ArgumentNullException.ThrowIfNull(logger);

            ApiUrl = apiUrl;
        }
    }
}
