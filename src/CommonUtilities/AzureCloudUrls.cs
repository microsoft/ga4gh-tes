// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json;
using System.Text.Json.Serialization;

namespace CommonUtilities.AzureCloud
{
    public class AzureCloudConfig
    {
        private const string defaultAzureCloudMetadataUrl = "https://management.azure.com/metadata/endpoints?api-version=2023-11-01";

        [JsonPropertyName("portal")]
        public string PortalUrl { get; set; }

        [JsonPropertyName("authentication")]
        public AuthenticationDetails Authentication { get; set; }

        [JsonPropertyName("media")]
        public string MediaUrl { get; set; }

        [JsonPropertyName("graphAudience")]
        public string GraphAudienceUrl { get; set; }

        [JsonPropertyName("graph")]
        public string GraphUrl { get; set; }

        [JsonPropertyName("name")]
        public string Name { get; set; }

        [JsonPropertyName("suffixes")]
        public EndpointSuffixes Suffixes { get; set; }

        [JsonPropertyName("batch")]
        public string BatchUrl { get; set; }

        [JsonPropertyName("resourceManager")]
        public string ResourceManagerUrl { get; set; }

        [JsonPropertyName("vmImageAliasDoc")]
        public string VmImageAliasDocumentationUrl { get; set; }

        [JsonPropertyName("sqlManagement")]
        public string SqlManagementUrl { get; set; }

        [JsonPropertyName("microsoftGraphResourceId")]
        public string MicrosoftGraphResourceUrl { get; set; }

        [JsonPropertyName("appInsightsResourceId")]
        public string ApplicationInsightsResourceUrl { get; set; }

        [JsonPropertyName("appInsightsTelemetryChannelResourceId")]
        public string ApplicationInsightsTelemetryChannelResourceUrl { get; set; }

        [JsonPropertyName("synapseAnalyticsResourceId")]
        public string SynapseAnalyticsResourceUrl { get; set; }

        [JsonPropertyName("logAnalyticsResourceId")]
        public string LogAnalyticsResourceUrl { get; set; }

        [JsonPropertyName("ossrDbmsResourceId")]
        public string OssrDbmsResourceUrl { get; set; }

        public static async Task<AzureCloudConfig> CreateAsync(string azureCloudMetadataUrl = defaultAzureCloudMetadataUrl)
        {
            using var httpClient = new HttpClient();

            try
            {
                var response = await httpClient.GetAsync(azureCloudMetadataUrl);
                response.EnsureSuccessStatusCode();
                var jsonString = await response.Content.ReadAsStringAsync();
                return JsonSerializer.Deserialize<AzureCloudConfig>(jsonString)!;
            }
            catch (HttpRequestException e)
            {
                throw new Exception("Error getting AzureCloudConfig", e);
            }
            catch (JsonException e)
            {
                throw new Exception("Error deserializing JSON data for AzureCloudConfig", e);
            }
        }
    }

    public class AuthenticationDetails
    {
        [JsonPropertyName("loginEndpoint")]
        public string LoginEndpointUrl { get; set; } = "https://login.microsoftonline.com";

        [JsonPropertyName("audiences")]
        public List<string> Audiences { get; set; }

        [JsonPropertyName("tenant")]
        public string Tenant { get; set; }

        [JsonPropertyName("identityProvider")]
        public string IdentityProvider { get; set; }
    }

    public class EndpointSuffixes
    {
        [JsonPropertyName("acrLoginServer")]
        public string AcrLoginServerSuffix { get; set; }

        [JsonPropertyName("sqlServerHostname")]
        public string SqlServerHostnameSuffix { get; set; }

        [JsonPropertyName("keyVaultDns")]
        public string KeyVaultDnsSuffix { get; set; }

        [JsonPropertyName("storage")]
        public string StorageSuffix { get; set; }

        [JsonPropertyName("storageSyncEndpointSuffix")]
        public string StorageSyncEndpointSuffix { get; set; }

        [JsonPropertyName("mhsmDns")]
        public string ManagedHsmDnsSuffix { get; set; }

        [JsonPropertyName("mysqlServerEndpoint")]
        public string MysqlServerEndpointSuffix { get; set; }

        [JsonPropertyName("postgresqlServerEndpoint")]
        public string PostgresqlServerEndpointSuffix { get; set; }

        [JsonPropertyName("mariadbServerEndpoint")]
        public string MariadbServerEndpointSuffix { get; set; }

        [JsonPropertyName("synapseAnalytics")]
        public string SynapseAnalyticsSuffix { get; set; }
    }



}
