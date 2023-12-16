// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using CommonUtilities.Options;
using Azure.Core;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Tes.ApiClients.Models.Terra;
using TesApi.Web.Management.Models.Terra;

namespace Tes.ApiClients
{
    /// <summary>
    /// Terra Workspace Manager api client
    /// </summary>
    public class TerraWsmApiClient : TerraApiClient
    {
        private const string WsmApiSegments = @"/api/workspaces/v1";
        private static readonly IMemoryCache sharedMemoryCache = new MemoryCache(new MemoryCacheOptions());

        /// <summary>
        /// Constructor of TerraWsmApiClient
        /// </summary>
        /// <param name="apiUrl">WSM API host</param>
        /// <param name="tokenCredential"></param>
        /// <param name="cachingRetryHandler"></param>
        /// <param name="logger"></param>
        public TerraWsmApiClient(string apiUrl, TokenCredential tokenCredential, CachingRetryHandler cachingRetryHandler,
            ILogger<TerraWsmApiClient> logger) : base(apiUrl, tokenCredential, cachingRetryHandler, logger)
        {

        }

        public static TerraWsmApiClient CreateTerraWsmApiClient(string apiUrl, TokenCredential tokenCredential)
        {
            var retryPolicyOptions = new RetryPolicyOptions();
            var cacheRetryHandler = new CachingRetryHandler(sharedMemoryCache,
                 Microsoft.Extensions.Options.Options.Create(retryPolicyOptions));

            return new TerraWsmApiClient(apiUrl, tokenCredential, cacheRetryHandler, ApiClientsLoggerFactory.Create<TerraWsmApiClient>());
        }

        /// <summary>
        /// Protected parameter-less constructor
        /// </summary>
        protected TerraWsmApiClient() { }

        /// <summary>
        /// Returns storage containers in the workspace.
        /// </summary>
        /// <param name="workspaceId">Terra workspace id</param>
        /// <param name="offset">Number of items to skip before starting to collect the result</param>
        /// <param name="limit">Maximum number of items to return</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public virtual async Task<WsmListContainerResourcesResponse> GetContainerResourcesAsync(Guid workspaceId, int offset, int limit, CancellationToken cancellationToken)
        {
            var url = GetContainerResourcesApiUrl(workspaceId, offset, limit);

            var response = await HttpSendRequestWithRetryPolicyAsync(() => new HttpRequestMessage(HttpMethod.Get, url),
                cancellationToken, setAuthorizationHeader: true);

            return await GetApiResponseContentAsync<WsmListContainerResourcesResponse>(response, cancellationToken);
        }

        private Uri GetContainerResourcesApiUrl(Guid workspaceId, int offset, int limit)
        {
            var segments = "/resources";

            var builder = GetWsmUriBuilder(workspaceId, segments);

            //TODO: add support for resource and stewardship parameters if required later 
            builder.Query = $"offset={offset}&limit={limit}&resource=AZURE_STORAGE_CONTAINER&stewardship=CONTROLLED";

            return builder.Uri;
        }

        /// <summary>
        /// Returns the SAS token of a container or blob for WSM managed storage account.
        /// </summary>
        /// <param name="workspaceId">Terra workspace id</param>
        /// <param name="resourceId">Terra resource id</param>
        /// <param name="sasTokenApiParameters">Sas token parameters</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public virtual async Task<WsmSasTokenApiResponse> GetSasTokenAsync(Guid workspaceId, Guid resourceId, SasTokenApiParameters sasTokenApiParameters, CancellationToken cancellationToken)
        {
            var url = GetSasTokenApiUrl(workspaceId, resourceId, sasTokenApiParameters);

            var response = await HttpSendRequestWithRetryPolicyAsync(() => new HttpRequestMessage(HttpMethod.Post, url),
                cancellationToken, setAuthorizationHeader: true);

            return await GetApiResponseContentAsync<WsmSasTokenApiResponse>(response, cancellationToken);
        }

        /// <summary>
        ///  Creates a Wsm managed batch pool
        /// </summary>
        /// <param name="workspaceId">Wsm Workspace id</param>
        /// <param name="apiCreateBatchPool">Create batch pool request</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public virtual async Task<ApiCreateBatchPoolResponse> CreateBatchPool(Guid workspaceId, ApiCreateBatchPoolRequest apiCreateBatchPool, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(apiCreateBatchPool);

            HttpResponseMessage response = null!;
            try
            {
                var uri = GetCreateBatchPoolUrl(workspaceId);

                Logger.LogInformation($"Creating a batch pool using WSM for workspace: {workspaceId}");

                response =
                    await HttpSendRequestWithRetryPolicyAsync(() => new HttpRequestMessage(HttpMethod.Post, uri) { Content = GetBatchPoolRequestContent(apiCreateBatchPool) },
                        cancellationToken, setAuthorizationHeader: true);

                var apiResponse = await GetApiResponseContentAsync<ApiCreateBatchPoolResponse>(response, cancellationToken);

                Logger.LogInformation($"Successfully created a batch pool using WSM for workspace: {workspaceId}");

                return apiResponse;
            }
            catch (Exception ex)
            {
                await LogResponseContentAsync(response, "Failed to create a Batch Pool via WSM", ex, cancellationToken);
                throw;
            }
        }

        /// <summary>
        /// Deletes a batch pool using the WSM API
        /// </summary>
        /// <param name="workspaceId">WSM workspace id</param>
        /// <param name="wsmBatchPoolResourceId">WSM resource id</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        public virtual async Task DeleteBatchPoolAsync(Guid workspaceId, Guid wsmBatchPoolResourceId, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(workspaceId);
            ArgumentNullException.ThrowIfNull(wsmBatchPoolResourceId);

            HttpResponseMessage response = null!;
            try
            {
                var uri = GetDeleteBatchPoolUrl(workspaceId, wsmBatchPoolResourceId);

                Logger.LogInformation($"Deleting the Batch pool using WSM for workspace: {workspaceId} WSM resource ID: {wsmBatchPoolResourceId}");

                response =
                    await HttpSendRequestWithRetryPolicyAsync(() => new HttpRequestMessage(HttpMethod.Delete, uri),
                        cancellationToken, setAuthorizationHeader: true);

                response.EnsureSuccessStatusCode();

                Logger.LogInformation($"Successfully deleted Batch pool, WSM resource ID: {wsmBatchPoolResourceId} using WSM for workspace: {workspaceId}");
            }
            catch (Exception ex)
            {
                await LogResponseContentAsync(response, "Failed to delete the Batch pool via WSM", ex, cancellationToken);
                throw;
            }
        }

        /// <summary>
        /// Gets the WSM delete batch pool URL
        /// </summary>
        /// <param name="workspaceId">WSM workspace id</param>
        /// <param name="wsmBatchPoolResourceId">WSM batch pool resource id</param>
        /// <returns></returns>
        public string GetDeleteBatchPoolUrl(Guid workspaceId, Guid wsmBatchPoolResourceId)
        {
            var segments = $"/resources/controlled/azure/batchpool/{wsmBatchPoolResourceId}";

            var builder = GetWsmUriBuilder(workspaceId, segments);

            return builder.Uri.AbsoluteUri;
        }

        /// <summary>
        /// Returns a parsed URL to get quota of a resource using the Terra Workspace Manager API.
        /// </summary>
        /// <param name="workspaceId">Workspace id</param>
        /// <param name="resourceId">Fully qualified Azure resource id</param>
        /// <returns></returns>
        public Uri GetQuotaApiUrl(Guid workspaceId, string resourceId)
        {
            var uriBuilder = GetWsmUriBuilder(workspaceId, "/resources/controlled/azure/landingzone/quota");
            uriBuilder.Query = $@"azureResourceId={Uri.EscapeDataString(resourceId)}";
            return uriBuilder.Uri;
        }

        /// <summary>
        /// Gets quota information for a given resource. 
        /// </summary>
        /// <param name="workspaceId">workspace id</param>
        /// <param name="resourceId">The fully qualified ID of the Azure resource, including the resource name and resource type.
        /// Use the format, /subscriptions/{guid}/resourceGroups/{resource-group-name}/{resource-provider-namespace}/{resource-type}/{resource-name}.</param>
        /// <param name="cacheResults"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public virtual async Task<QuotaApiResponse> GetResourceQuotaAsync(Guid workspaceId, string resourceId, bool cacheResults, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(workspaceId);
            ArgumentException.ThrowIfNullOrEmpty(resourceId);

            var url = GetQuotaApiUrl(workspaceId, resourceId);

            return await HttpGetRequestAsync<QuotaApiResponse>(url, setAuthorizationHeader: true, cacheResults: cacheResults, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Returns a parsed URL to get resources in a landing zone using the Terra Workspace Manager API.
        /// </summary>
        /// <param name="workspaceId">workspace id</param>
        /// <returns></returns>
        public Uri GetLandingZoneResourcesApiUrl(Guid workspaceId)
        {
            var uriBuilder = GetWsmUriBuilder(workspaceId, "/resources/controlled/azure/landingzone");
            return uriBuilder.Uri;
        }

        /// <summary>
        /// List the resources in a landing zone. 
        /// </summary>
        /// <param name="workspaceId"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="cacheResults"></param>
        /// <returns></returns>
        public virtual async Task<LandingZoneResourcesApiResponse> GetLandingZoneResourcesAsync(Guid workspaceId, CancellationToken cancellationToken, bool cacheResults = true)
        {
            ArgumentNullException.ThrowIfNull(workspaceId);

            var url = GetLandingZoneResourcesApiUrl(workspaceId);

            return await HttpGetRequestAsync<LandingZoneResourcesApiResponse>(url, setAuthorizationHeader: true, cacheResults: cacheResults, cancellationToken: cancellationToken);
        }


        private async Task LogResponseContentAsync(HttpResponseMessage response, string errMessage, Exception ex, CancellationToken cancellationToken)
        {
            var responseContent = string.Empty;

            if (response is not null)
            {
                responseContent = await ReadResponseBodyAsync(response, cancellationToken);
            }

            Logger.LogError(ex, $"{errMessage}. Response content:{responseContent}");
        }

        private string GetCreateBatchPoolUrl(Guid workspaceId)
        {
            var segments = $"/resources/controlled/azure/batchpool";

            var builder = GetWsmUriBuilder(workspaceId, segments);

            return builder.Uri.AbsoluteUri;
        }

        private static HttpContent GetBatchPoolRequestContent(ApiCreateBatchPoolRequest apiCreateBatchPool)
            => new StringContent(JsonSerializer.Serialize(apiCreateBatchPool,
                new JsonSerializerOptions() { DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull }), Encoding.UTF8, "application/json");

        private static string ToQueryString(SasTokenApiParameters sasTokenApiParameters)
            => AppendQueryStringParams(
                ParseQueryStringParameter("sasIpRange", sasTokenApiParameters.SasIpRange),
                ParseQueryStringParameter("sasExpirationDuration", sasTokenApiParameters.SasExpirationInSeconds.ToString()),
                ParseQueryStringParameter("sasPermissions", sasTokenApiParameters.SasPermission),
                ParseQueryStringParameter("sasBlobName", sasTokenApiParameters.SasBlobName));

        /// <summary>
        /// Gets the Api Url to get a container or blob sas token
        /// </summary>
        /// <param name="workspaceId">Workspace Id</param>
        /// <param name="resourceId">WSM resource Id of the container</param>
        /// <param name="sasTokenApiParameters"><see cref="SasTokenApiParameters"/></param>
        /// <returns></returns>
        public virtual Uri GetSasTokenApiUrl(Guid workspaceId, Guid resourceId, SasTokenApiParameters sasTokenApiParameters)
        {
            var segments = $"/resources/controlled/azure/storageContainer/{resourceId}/getSasToken";

            var builder = GetWsmUriBuilder(workspaceId, segments);

            if (sasTokenApiParameters != null)
            {
                builder.Query = ToQueryString(sasTokenApiParameters);
            }

            return builder.Uri;
        }

        private UriBuilder GetWsmUriBuilder(Guid workspaceId, string pathSegments)
        {
            //This is okay given the perf expectations of service  - no current need to optimize string allocations.
            var apiRequestUrl = $"{ApiUrl.TrimEnd('/')}{WsmApiSegments}/{workspaceId}{pathSegments}";

            var uriBuilder = new UriBuilder(apiRequestUrl);

            return uriBuilder;
        }
    }
}
