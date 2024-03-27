// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using CommonUtilities;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Tes.ApiClients.Models.Terra;

namespace Tes.ApiClients
{
    public class DrsHubApiClient : TerraApiClient
    {
        private const string DrsHubApiSegments = "/api/v4/drs";       
        private static readonly IMemoryCache SharedMemoryCache = new MemoryCache(new MemoryCacheOptions());

        /// <summary>
        /// Parameterless constructor for mocking
        /// </summary>
        protected DrsHubApiClient()
        {
        }

        /// <summary>
        /// Constructor of DrsHubApiClient
        /// </summary>
        /// <param name="apiUrl">WSM API host</param>
        /// <param name="tokenCredential"></param>
        /// <param name="cachingRetryPolicyBuilder"></param>
        /// <param name="azureCloudIdentityConfig"></param>
        /// <param name="logger"></param>
        public DrsHubApiClient(string apiUrl, TokenCredential tokenCredential, CachingRetryPolicyBuilder cachingRetryPolicyBuilder,
            AzureEnvironmentConfig azureCloudIdentityConfig, ILogger<DrsHubApiClient> logger) : base(apiUrl, tokenCredential, cachingRetryPolicyBuilder, azureCloudIdentityConfig, logger)
        {

        }

        public static DrsHubApiClient CreateDrsHubApiClient(string apiUrl, TokenCredential tokenCredential, AzureEnvironmentConfig azureCloudIdentityConfig)
        {            
            return CreateTerraApiClient<DrsHubApiClient>(apiUrl, SharedMemoryCache, tokenCredential,azureCloudIdentityConfig);
        }

        public virtual async Task<DrsResolveApiResponse> ResolveDrsUriAsync(Uri drsUri, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(drsUri);

            HttpResponseMessage response = null!;
            try
            {
                var apiUrl = GetResolveDrsApiUrl();

                Logger.LogInformation(@"Resolving DRS URI calling: {uri}", apiUrl);

                response =
                    await HttpSendRequestWithRetryPolicyAsync(() => new HttpRequestMessage(HttpMethod.Post, apiUrl) { Content = GetDrsResolveRequestContent(drsUri) },
                        cancellationToken, setAuthorizationHeader: true);

                var apiResponse = await GetDrsResolveApiResponseAsync(response, cancellationToken);

                Logger.LogInformation(@"Successfully resolved URI: {drsUri}", drsUri);

                return apiResponse;
            }
            catch (Exception ex)
            {
                await LogResponseContentAsync(response, "Failed to resolve DRS URI", ex, cancellationToken);
                throw;
            }
        }

        public static async Task<DrsResolveApiResponse> GetDrsResolveApiResponseAsync(HttpResponseMessage response, CancellationToken cancellationToken)
        {
            var apiResponse = await GetApiResponseContentAsync<DrsResolveApiResponse>(response, cancellationToken);
            return apiResponse;
        }

        public HttpContent GetDrsResolveRequestContent(Uri drsUri)
        {        
            ArgumentNullException.ThrowIfNull(drsUri);

            var drsResolveApiRequestBody = new DrsResolveRequestContent
            {
                    Url = drsUri.AbsoluteUri,
                    CloudPlatform = CloudPlatform.Azure,
                    Fields = new List<string> { "accessUrl" }
            };

            return CreateJsonStringContent(drsResolveApiRequestBody);
        }

        private string GetResolveDrsApiUrl()
        {
            var apiRequestUrl = $"{ApiUrl.TrimEnd('/')}{DrsHubApiSegments}/resolve";

            var builder = new UriBuilder(apiRequestUrl);

            return builder.Uri.AbsoluteUri;
        }
    }
}
