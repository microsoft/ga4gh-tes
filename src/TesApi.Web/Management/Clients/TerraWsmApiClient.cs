// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
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

            HttpResponseMessage response = null;
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

            HttpResponseMessage response = null;
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

        private async Task LogResponseContentAsync(HttpResponseMessage response, string errMessage, Exception ex, CancellationToken cancellationToken)
        {
            var responseContent = "";
            if (response is not null)
            {
                responseContent = await response.Content.ReadAsStringAsync(cancellationToken);
            }

            Logger.LogError(ex, $"{errMessage}. Response content:{responseContent}");
        }

        private string GetCreateBatchPoolUrl(Guid workspaceId)
        {
            var segments = $"/resources/controlled/azure/batchpool";

            var builder = GetWsmUriBuilder(workspaceId, segments);

            return builder.Uri.AbsoluteUri;
        }

        private HttpContent GetBatchPoolRequestContent(ApiCreateBatchPoolRequest apiCreateBatchPool)
            => new StringContent(JsonSerializer.Serialize(apiCreateBatchPool,
                new JsonSerializerOptions() { DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull }), Encoding.UTF8, "application/json");

        private string ToQueryString(SasTokenApiParameters sasTokenApiParameters)
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
            var apiRequestUrl = $"{baseApiUrl}{workspaceId}{pathSegments}";

            var uriBuilder = new UriBuilder(apiRequestUrl);

            return uriBuilder;
        }
    }
}
