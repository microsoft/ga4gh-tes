// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.ResourceManager;
using Microsoft.Extensions.Options;

namespace CommonUtilities
{
    /// <summary>
    /// Azure cloud endpoints
    /// </summary>
    /// <param name="AuthorityHost">Similar to "https://login.microsoftonline.com".</param>
    /// <param name="Audience">Similar to "https://management.core.windows.net/".</param>
    /// <param name="Tenant">Similar to "common".</param>
    /// <param name="ResourceManager">Similar to "https://management.azure.com/".</param>
    /// <param name="AppInsightsResource">Similar to "https://api.applicationinsights.io".</param>
    /// <param name="AppInsightsTelemetry">Similar to "https://dc.applicationinsights.azure.com/v2/track".</param>
    /// <param name="BatchResource">Similar to "https://batch.core.windows.net/".</param>
    /// <param name="AcrSuffix">Similar to "azurecr.io".</param>
    /// <param name="KeyVaultSuffix">Similar to "vault.azure.net".</param>
    /// <param name="StorageSuffix">Similar to "core.windows.net".</param>
    /// <param name="PostgresqlSuffix">Similar to "postgres.database.azure.com".</param>
    public record class ArmEnvironmentEndpoints(Uri AuthorityHost, string Audience, string Tenant, Uri ResourceManager, Uri AppInsightsResource, Uri AppInsightsTelemetry, Uri BatchResource, string AcrSuffix, string KeyVaultSuffix, string StorageSuffix, string PostgresqlSuffix)
    {
        /// <summary>
        /// Gets default authentication scope.
        /// </summary>
        public string DefaultScope => $"{Audience}/.default";


        /// <summary>
        /// Azure cloud endpoints from cloud management endpoints
        /// </summary>
        /// <param name="cloudManagement">Azure cloud resource management endpoint.</param>
        /// <returns></returns>
        public static async Task<ArmEnvironmentEndpoints> FromMetadataEndpointsAsync(Uri cloudManagement, IOptions<Options.RetryPolicyOptions> retryPolicyOptions)
        {
            ArgumentNullException.ThrowIfNull(cloudManagement);
            var retryPolicy = new RetryPolicyBuilder(retryPolicyOptions).DefaultRetryHttpResponseMessagePolicyBuilder().SetOnRetryBehavior().AsyncBuildPolicy();
            System.Text.Json.JsonDocument endpointMetadata;

            {
                HttpResponseMessage response;

                {
                    using HttpClient client = new();
                    response = await retryPolicy.ExecuteAsync(() =>
                        client.SendAsync(new(HttpMethod.Get, new UriBuilder(cloudManagement) { Path = "/metadata/endpoints", Query = "api-version=2023-11-01" }.Uri)));
                }

                response.EnsureSuccessStatusCode();

                {
                    using var stream = response.Content.ReadAsStream();
                    endpointMetadata = await System.Text.Json.JsonDocument.ParseAsync(stream);
                }
            }

            using var cloud = endpointMetadata;
            return ParseJson(cloud);
        }

        /// <summary>
        /// Azure cloud endpoints from cloud name
        /// </summary>
        /// <param name="cloudName">Name of Azure cloud, either from IMDS or the resource manager metadata/endpoints query.</param>
        /// <returns><see cref="ArmEnvironmentEndpoints"/>.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        /// <remarks>
        /// Recognized cloud names are:
        /// <para>All generally available global Azure regions: <c>AzureCloud</c>, <c>AzurePublicCloud</c></para>
        /// <para>Azure Government: <c>AzureUSGovernment</c>: <c>AzureUSGovernmentCloud</c></para>
        /// <para>Microsoft Azure operated by 21Vianet: <c>AzureChinaCloud</c></para>
        /// </remarks>
        public static Task<ArmEnvironmentEndpoints> FromKnownCloudNameAsync(string cloudName, IOptions<Options.RetryPolicyOptions> retryPolicyOptions) =>
            cloudName.ToLowerInvariant() switch
            {
                "azurepubliccloud" => FromMetadataEndpointsAsync(AzurePublicCloud, retryPolicyOptions),
                "azurecloud" => FromMetadataEndpointsAsync(AzurePublicCloud, retryPolicyOptions),
                "azureusgovernmentcloud" => FromMetadataEndpointsAsync(AzureUSGovernmentCloud, retryPolicyOptions),
                "azureusgovernment" => FromMetadataEndpointsAsync(AzureUSGovernmentCloud, retryPolicyOptions),
                "azurechinacloud" => FromMetadataEndpointsAsync(AzureChinaCloud, retryPolicyOptions),
                null => throw new ArgumentNullException(nameof(cloudName)),
                _ => throw new ArgumentOutOfRangeException(nameof(cloudName)),
            };

        private static readonly Uri AzurePublicCloud = ArmEnvironment.AzurePublicCloud.Endpoint;
        private static readonly Uri AzureUSGovernmentCloud = ArmEnvironment.AzureGovernment.Endpoint;
        private static readonly Uri AzureChinaCloud = ArmEnvironment.AzureChina.Endpoint;

        private static ArmEnvironmentEndpoints ParseJson(System.Text.Json.JsonDocument cloud)
        {
            var root = cloud.RootElement;
            Console.WriteLine($"Using Azure cloud: {root.GetProperty("name").GetString()}");

            var authentication = root.GetProperty("authentication");

            if (!"AAD".Equals(authentication.GetProperty("identityProvider").GetString(), StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException("This azure cloud is not supported. 'identityProvider' must be 'AAD'");
            }

            var loginEndpoint = GetPropertyString(authentication, "loginEndpoint");
            var audience = authentication.GetProperty("audiences").EnumerateArray().Select(e => e.GetString()).Last(e => !string.IsNullOrWhiteSpace(e))!;
            var tenant = GetPropertyString(authentication, "tenant");
            var resourceManager = GetPropertyString(root, "resourceManager");
            var appInsightsResourceId = GetPropertyString(root, "appInsightsResourceId");
            var appInsightsTelemetryChannelResourceId = GetPropertyString(root, "appInsightsTelemetryChannelResourceId");
            var batch = GetPropertyString(root, "batch");
            var suffixes = root.GetProperty("suffixes");
            var acrLoginServer = GetPropertyString(suffixes, "acrLoginServer");
            var keyVaultDns = GetPropertyString(suffixes, "keyVaultDns");
            var storage = GetPropertyString(suffixes, "storage");
            var postgresqlServerEndpoint = GetPropertyString(suffixes, "postgresqlServerEndpoint");

            return new(new(loginEndpoint), audience, tenant, new(resourceManager), new(appInsightsResourceId), new(appInsightsTelemetryChannelResourceId), new(batch), acrLoginServer, keyVaultDns, storage, postgresqlServerEndpoint);

            static string GetPropertyString(System.Text.Json.JsonElement element, string key) => element.GetProperty(key).GetString() ?? throw new InvalidOperationException($"This azure cloud is not supported: '{key}' is empty.");
        }
    }
}
