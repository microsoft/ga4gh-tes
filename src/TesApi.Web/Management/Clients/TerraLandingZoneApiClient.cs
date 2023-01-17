﻿using System;
using System.Threading.Tasks;
using Azure.Core;
using Microsoft.Extensions.Logging;
using TesApi.Web.Management.Models.Terra;

namespace TesApi.Web.Management.Clients
{
    /// <summary>
    /// Terra Landing Zone api client. 
    /// </summary>
    public class TerraLandingZoneApiClient : HttpApiClient
    {
        private const string LandingZonesApiSegments = @"/api/landingzones/v1/azure";
        private const string TokenScope = @"https://management.azure.com/.default";

        private readonly string baseApiUrl;

        /// <summary>
        /// Constructor of TerraLandingZoneApiClient
        /// </summary>
        /// <param name="apiHost"></param>
        /// <param name="tokenCredential"></param>
        /// <param name="cacheAndRetryHandler"></param>
        /// <param name="logger"></param>
        public TerraLandingZoneApiClient(string apiHost, TokenCredential tokenCredential, CacheAndRetryHandler cacheAndRetryHandler, ILogger<TerraLandingZoneApiClient> logger) : base(tokenCredential, TokenScope, cacheAndRetryHandler, logger)
        {
            ArgumentException.ThrowIfNullOrEmpty(apiHost);
            ArgumentNullException.ThrowIfNull(tokenCredential);
            ArgumentNullException.ThrowIfNull(cacheAndRetryHandler);

            this.baseApiUrl = apiHost.TrimEnd('/') + LandingZonesApiSegments;
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
        /// <returns></returns>
        public virtual async Task<QuotaApiResponse> GetResourceQuotaAsync(Guid landingZoneId, string resourceId, bool cacheResults)
        {
            ArgumentNullException.ThrowIfNull(landingZoneId);
            ArgumentException.ThrowIfNullOrEmpty(resourceId);

            var url = GetQuotaApiUrl(landingZoneId, resourceId);

            return await HttpGetRequestAsync<QuotaApiResponse>(url, setAuthorizationHeader: true, cacheResults);
        }


        /// <summary>
        /// List the resources in a landing zone. 
        /// </summary>
        /// <param name="landingZoneId"></param>
        /// <param name="cacheResults"></param>
        /// <returns></returns>
        public virtual async Task<LandingZoneResourcesApiResponse> GetLandingZoneResourcesAsync(Guid landingZoneId, bool cacheResults = true)
        {
            ArgumentNullException.ThrowIfNull(landingZoneId);

            var url = GetLandingZoneResourcesApiUrl(landingZoneId);

            return await HttpGetRequestAsync<LandingZoneResourcesApiResponse>(url, setAuthorizationHeader: true, cacheResults);

        }

        private Uri GetQuotaApiUrl(Guid landingZoneId, string resourceId)
        {
            var uriBuilder = GetLandingZoneUriBuilder(landingZoneId, "/resource-quota");
            uriBuilder.Query = $@"azureResourceId={resourceId}";
            return uriBuilder.Uri;
        }

        private Uri GetLandingZoneResourcesApiUrl(Guid landingZoneId)
        {
            var uriBuilder = GetLandingZoneUriBuilder(landingZoneId, "/resource");
            return uriBuilder.Uri;
        }

        private UriBuilder GetLandingZoneUriBuilder(Guid landingZoneId, string pathSegments)
        {
            //This is okay given the perf expectations of service  - no current need to optimize string allocations.
            var apiRequestUrl = baseApiUrl + landingZoneId.ToString() + pathSegments;
            var uriBuilder = new UriBuilder(apiRequestUrl);
            return uriBuilder;
        }

    }
}
