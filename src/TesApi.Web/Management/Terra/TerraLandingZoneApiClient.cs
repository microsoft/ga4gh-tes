using System;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace TesApi.Web.Management.Terra
{
    /// <summary>
    /// Terra Landing Zone api client. 
    /// </summary>
    public class TerraLandingZoneApiClient
    {
        private const string LandingZonesApiSegments = "/api/landingzones/v1/azure";

        private readonly IHttpClientWrapper httpClientWrapper;
        private readonly string baseApiUrl;
        private readonly ILogger logger;


        /// <summary>
        ///  Constructor of TerraLandingZoneApiClient
        /// </summary>
        /// <param name="httpClientWrapper">HTTP client wrapper</param>
        /// <param name="apiHost">Terra api host</param>
        /// <param name="logger">Logger instance</param>
        public TerraLandingZoneApiClient(IHttpClientWrapper httpClientWrapper, string apiHost, ILogger logger)
        {
            ArgumentNullException.ThrowIfNull(httpClientWrapper);
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentException.ThrowIfNullOrEmpty(apiHost);


            this.httpClientWrapper = httpClientWrapper;
            this.logger = logger;
            this.baseApiUrl = apiHost.TrimEnd('/') + LandingZonesApiSegments;
        }


        /// <summary>
        /// Gets quota information for a given resource. 
        /// </summary>
        /// <param name="landingZoneId">landing zone id</param>
        /// <param name="resourceId">The fully qualified ID of the Azure resource, including the resource name and resource type.
        /// Use the format, /subscriptions/{guid}/resourceGroups/{resource-group-name}/{resource-provider-namespace}/{resource-type}/{resource-name}.</param>
        /// <returns></returns>
        public async Task<QuotaApiResponse> GetResourceQuotaAsync(Guid landingZoneId, string resourceId)
        {
            ArgumentNullException.ThrowIfNull(landingZoneId);
            ArgumentException.ThrowIfNullOrEmpty(resourceId);

            var httpRequest = GetQuotaApiHttpRequest(landingZoneId, resourceId);

            return await SendApiRequestAsync<QuotaApiResponse>(httpRequest, true);
        }


        /// <summary>
        /// List the resources in a landing zone. 
        /// </summary>
        /// <param name="landingZoneId"></param>
        /// <returns></returns>
        public async Task<LandingZoneResourcesApiResponse> GetLandingZoneResourcesAsync(Guid landingZoneId)
        {
            ArgumentNullException.ThrowIfNull(landingZoneId);

            var httpRequest = GetLandingZoneResourcesHttpRequest(landingZoneId);

            return await SendApiRequestAsync<LandingZoneResourcesApiResponse>(httpRequest, true);
        }

        private async Task<TResponse> SendApiRequestAsync<TResponse>(HttpRequestMessage httpRequest,
            bool setAuthorizationHeader)
        {
            try
            {
                logger.LogInformation($"Making request to the landing zone api. Url: {httpRequest.RequestUri}");

                var response = await httpClientWrapper.SendRequestWithCachingAndRetryPolicyAsync(httpRequest, setAuthorizationHeader);

                logger.LogInformation($"Successfully called the landing zone api. Url: {httpRequest.RequestUri}");

                return await ToApiResponse<TResponse>(response);
            }
            catch (Exception e)
            {
                logger.LogError($"Failed to call the landing zone api. Terra URL:{httpRequest.RequestUri}",
                    e);
                throw;
            }
        }

        private async Task<T> ToApiResponse<T>(HttpResponseMessage response)
        {
            var body = await response.Content.ReadAsStringAsync();

            return JsonSerializer.Deserialize<T>(body);
        }

        private HttpRequestMessage GetQuotaApiHttpRequest(Guid landingZoneId, string resourceId)
        {

            var uriBuilder = GetLandingZoneUriBuilder(landingZoneId, "/resource-quota");
            uriBuilder.Query = $@"azureResourceId={resourceId}";
            return new HttpRequestMessage(HttpMethod.Get, uriBuilder.Uri);
        }

        private HttpRequestMessage GetLandingZoneResourcesHttpRequest(Guid landingZoneId)
        {
            var uriBuilder = GetLandingZoneUriBuilder(landingZoneId, "/resource");
            return new HttpRequestMessage(HttpMethod.Get, uriBuilder.Uri);
        }

        private UriBuilder GetLandingZoneUriBuilder(Guid landingZoneId, string pathSegments)
        {
            //This is okay given the perf expectations of service, so there's no need to optimize string allocations.
            var apiRequestUrl = baseApiUrl + landingZoneId.ToString() + pathSegments;
            var uriBuilder = new UriBuilder(apiRequestUrl);
            return uriBuilder;
        }

    }
}
