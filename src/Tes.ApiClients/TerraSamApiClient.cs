// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using CommonUtilities;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Tes.ApiClients.Models.Terra;

namespace Tes.ApiClients
{
    /// <summary>
    /// Terra Sam api client
    /// Sam manages authorization and IAM functionality
    /// </summary>
    public class TerraSamApiClient : TerraApiClient
    {
        private const string SamApiSegments = @"/api/azure/v1";
        private static readonly IMemoryCache SharedMemoryCache = new MemoryCache(new MemoryCacheOptions());

        /// <summary>
        /// Constructor of TerraSamApiClient
        /// </summary>
        /// <param name="apiUrl">Sam API host</param>
        /// <param name="tokenCredential"></param>
        /// <param name="cachingRetryHandler"></param>
        /// <param name="azureCloudIdentityConfig"></param>
        /// <param name="logger"></param>
        public TerraSamApiClient(string apiUrl, TokenCredential tokenCredential, CachingRetryPolicyBuilder cachingRetryHandler,
            AzureEnvironmentConfig azureCloudIdentityConfig, ILogger<TerraSamApiClient> logger) : base(apiUrl, tokenCredential, cachingRetryHandler, azureCloudIdentityConfig, logger)
        {

        }

        public static TerraSamApiClient CreateTerraSamApiClient(string apiUrl, TokenCredential tokenCredential, AzureEnvironmentConfig azureCloudIdentityConfig)
        {
            return CreateTerraApiClient<TerraSamApiClient>(apiUrl, SharedMemoryCache, tokenCredential, azureCloudIdentityConfig);
        }

        /// <summary>
        /// Protected parameter-less constructor
        /// </summary>
        protected TerraSamApiClient() { }

        public virtual async Task<SamActionManagedIdentityApiResponse> GetActionManagedIdentityForACRPullAsync(Guid billingProfileId, CancellationToken cancellationToken)
        {
            return await GetActionManagedIdentityAsync("private_azure_container_registry", billingProfileId, "pull_image", cancellationToken);
        }

        public virtual async Task<SamActionManagedIdentityApiResponse> GetActionManagedIdentityAsync(string resourceType, Guid resourceId, string action, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(resourceId);

            var url = GetSamActionManagedIdentityUrl(resourceType, resourceId, action);

            Logger.LogInformation(@"Fetching action managed identity from Sam for {resourceId}", resourceId);

            // TODO What happens if this request errors out?
            return 
                await HttpGetRequestAsync(url, setAuthorizationHeader: true, cacheResults: true, 
                    SamActionManagedIdentityApiResponseContext.Default.SamActionManagedIdentityApiResponse, cancellationToken); 
        }


        public virtual Uri GetSamActionManagedIdentityUrl(string resourceType, Guid resourceId, string action)
        {
            var apiRequestUrl = $"{ApiUrl.TrimEnd('/')}{SamApiSegments}/actionManagedIdentity/{resourceType}/{resourceId}/{action}";

            var uriBuilder = new UriBuilder(apiRequestUrl);

            return uriBuilder.Uri;
        }
    }
}
