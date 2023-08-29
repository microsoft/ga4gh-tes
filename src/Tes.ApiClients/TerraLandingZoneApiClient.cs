// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using Microsoft.Extensions.Logging;
using TesApi.Web.Management.Models.Terra;

namespace Tes.ApiClients
{
    /// <summary>
    /// Terra Landing Zone api client. 
    /// </summary>
    public class TerraLandingZoneApiClient : TerraApiClient
    {
        private const string LandingZonesApiSegments = @"/api/landingzones/v1/azure";

        /// <summary>
        /// Constructor of TerraLandingZoneApiClient
        /// </summary>
        /// <param name="apiUrl"></param>
        /// <param name="tokenCredential"></param>
        /// <param name="cachingRetryHandler"></param>
        /// <param name="logger"></param>
        public TerraLandingZoneApiClient(string apiUrl, TokenCredential tokenCredential, CachingRetryHandler cachingRetryHandler, ILogger<TerraLandingZoneApiClient> logger) : base(apiUrl, tokenCredential, cachingRetryHandler, logger)
        {
        }

        /// <summary>
        /// Protected parameter-less constructor of TerraLandingZoneApiClient
        /// </summary>
        protected TerraLandingZoneApiClient() { }

        /// <summary>
        /// Gets quota information for a given resource. 
        /// </summary>
        /// <param name="landingZoneId">landing zone id</param>
        /// <param name="resourceId">The fully qualified ID of the Azure resource, including the resource name and resource type.
        /// Use the format, /subscriptions/{guid}/resourceGroups/{resource-group-name}/{resource-provider-namespace}/{resource-type}/{resource-name}.</param>
        /// <param name="cacheResults"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public virtual async Task<QuotaApiResponse> GetResourceQuotaAsync(Guid landingZoneId, string resourceId, bool cacheResults, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(landingZoneId);
            ArgumentException.ThrowIfNullOrEmpty(resourceId);

            var url = GetQuotaApiUrl(landingZoneId, resourceId);

            return await HttpGetRequestAsync<QuotaApiResponse>(url, setAuthorizationHeader: true, cacheResults: cacheResults, cancellationToken: cancellationToken);
        }


        /// <summary>
        /// List the resources in a landing zone. 
        /// </summary>
        /// <param name="landingZoneId"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="cacheResults"></param>
        /// <returns></returns>
        public virtual async Task<LandingZoneResourcesApiResponse> GetLandingZoneResourcesAsync(Guid landingZoneId, CancellationToken cancellationToken, bool cacheResults = true)
        {
            ArgumentNullException.ThrowIfNull(landingZoneId);

            var url = GetLandingZoneResourcesApiUrl(landingZoneId);

            return await HttpGetRequestAsync<LandingZoneResourcesApiResponse>(url, setAuthorizationHeader: true, cacheResults: cacheResults, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Returns a parsed URL to get quota of a resource using the Terra landing zone API.
        /// </summary>
        /// <param name="landingZoneId">Landing zone id</param>
        /// <param name="resourceId">Fully qualified Azure resource id</param>
        /// <returns></returns>
        public Uri GetQuotaApiUrl(Guid landingZoneId, string resourceId)
        {
            var uriBuilder = GetLandingZoneUriBuilder(landingZoneId, "/resource-quota");
            uriBuilder.Query = $@"azureResourceId={Uri.EscapeDataString(resourceId)}";
            return uriBuilder.Uri;
        }

        /// <summary>
        /// Returns a parsed URL to get resources in a landing zone using the Terra landing zone API.
        /// </summary>
        /// <param name="landingZoneId">Landing zone id</param>
        /// <returns></returns>
        public Uri GetLandingZoneResourcesApiUrl(Guid landingZoneId)
        {
            var uriBuilder = GetLandingZoneUriBuilder(landingZoneId, "/resources");
            return uriBuilder.Uri;
        }

        private UriBuilder GetLandingZoneUriBuilder(Guid landingZoneId, string pathSegments)
        {
            //This is okay given the perf expectations of service  - no current need to optimize string allocations.
            var apiRequestUrl = $"{ApiUrl.TrimEnd('/')}{LandingZonesApiSegments}/{landingZoneId}{pathSegments}";
            var uriBuilder = new UriBuilder(apiRequestUrl);
            return uriBuilder;
        }
    }
}
