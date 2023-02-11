// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Azure.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TesApi.Web.Management.Configuration;
using TesApi.Web.Management.Models.Terra;

namespace TesApi.Web.Management.Clients
{
    /// <summary>
    /// Terra Workspace Manager api client
    /// </summary>
    public class TerraWsmApiClient : TerraApiClient
    {
        private const string WsmApiSegments = @"/api/workspaces/v1/";

        private readonly string baseApiUrl;

        /// <summary>
        /// Constructor of TerraWsmApiClient
        /// </summary>
        /// <param name="tokenCredential"></param>
        /// <param name="terraOptions"></param>
        /// <param name="cacheAndRetryHandler"></param>
        /// <param name="logger"></param>
        public TerraWsmApiClient(TokenCredential tokenCredential, IOptions<TerraOptions> terraOptions, CacheAndRetryHandler cacheAndRetryHandler, ILogger<TerraWsmApiClient> logger) : base(tokenCredential, cacheAndRetryHandler, logger)
        {
            ArgumentException.ThrowIfNullOrEmpty(terraOptions.Value.WsmApiHost, nameof(terraOptions.Value.WsmApiHost));

            this.baseApiUrl = terraOptions.Value.WsmApiHost.TrimEnd('/') + WsmApiSegments;
        }

        /// <summary>
        /// Protected parameter-less constructor
        /// </summary>
        protected TerraWsmApiClient() { }

        /// <summary>
        /// Returns the SAS token of a container or blob for WSM managed storage account.
        /// </summary>
        /// <param name="workspaceId">Terra workspace id</param>
        /// <param name="resourceId">Terra resource id</param>
        /// <param name="sasTokenApiParameters">Sas token parameters</param>
        /// <returns></returns>
        public virtual async Task<WsmSasTokenApiResponse> GetSasTokenAsync(Guid workspaceId, Guid resourceId, SasTokenApiParameters sasTokenApiParameters)
        {
            var url = GetContainerSasTokenApiUrl(workspaceId, resourceId, sasTokenApiParameters);

            var response = await HttpSendRequestWithRetryPolicyAsync(() => new HttpRequestMessage(HttpMethod.Post, url),
                setAuthorizationHeader: true);

            return await GetApiResponseContentAsync<WsmSasTokenApiResponse>(response);
        }

        /// <summary>
        ///  Creates a Wsm managed batch pool
        /// </summary>
        /// <param name="workspaceId">Wsm Workspace id</param>
        /// <param name="apiCreateBatchPool">Create batch pool request</param>
        /// <returns></returns>
        public virtual async Task<ApiCreateBatchPoolResponse> CreateBatchPool(Guid workspaceId, ApiCreateBatchPoolRequest apiCreateBatchPool)
        {
            ArgumentNullException.ThrowIfNull(apiCreateBatchPool);

            HttpResponseMessage response = null;
            try
            {
                var uri = GetCreateBatchPoolUrl(workspaceId);

                Logger.LogInformation($"Creating a batch pool using WSM for workspace:{workspaceId}");

                response =
                    await HttpSendRequestWithRetryPolicyAsync(() => new HttpRequestMessage(HttpMethod.Post, uri) { Content = GetBatchPoolRequestContent(apiCreateBatchPool) },
                        setAuthorizationHeader: true);

                Logger.LogInformation($"Successfully created a batch pool using WSM for workspace:{workspaceId}");

                return await GetApiResponseContentAsync<ApiCreateBatchPoolResponse>(response);
            }
            catch (Exception ex)
            {
                var responseContent = "";
                if (response is not null)
                {
                    responseContent = await response.Content.ReadAsStringAsync();
                }

                Logger.LogError(ex, $"Failed to create a batch pool using wsm. Response content:{responseContent}");
                throw;
            }
        }

        private string GetCreateBatchPoolUrl(Guid workspaceId)
        {
            var segments = $"/resources/controlled/azure/batchpool";

            var builder = GetWsmUriBuilder(workspaceId, segments);

            return builder.ToString();
        }

        private HttpContent GetBatchPoolRequestContent(ApiCreateBatchPoolRequest apiCreateBatchPool)
        {
            return new StringContent(JsonSerializer.Serialize(apiCreateBatchPool,
                new JsonSerializerOptions() { DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull }), Encoding.UTF8, "application/json");
        }

        private string ToQueryString(SasTokenApiParameters sasTokenApiParameters)
            => AppendQueryStringParams(
                ParseQueryStringParameter("sasIpRange", sasTokenApiParameters.SasIpRange),
                ParseQueryStringParameter("sasExpirationDuration", sasTokenApiParameters.SasExpirationInSeconds.ToString()),
                ParseQueryStringParameter("sasPermissions", sasTokenApiParameters.SasPermission),
                ParseQueryStringParameter("sasBlobName", sasTokenApiParameters.SasBlobName));

        /// <summary>
        /// Gets the Api Url to get a container sas token
        /// </summary>
        /// <param name="workspaceId">Workspace Id</param>
        /// <param name="resourceId">WSM resource Id of the container</param>
        /// <param name="sasTokenApiParameters"><see cref="SasTokenApiParameters"/></param>
        /// <returns></returns>
        public virtual Uri GetContainerSasTokenApiUrl(Guid workspaceId, Guid resourceId, SasTokenApiParameters sasTokenApiParameters)
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
            var apiRequestUrl = $"{baseApiUrl}{workspaceId}{pathSegments}";

            var uriBuilder = new UriBuilder(apiRequestUrl);

            return uriBuilder;
        }
    }
}
