// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json;
using System.Text.Json.Serialization;
using Polly;

namespace CommonUtilities.AzureCloud
{
    public class AzureCloudConfig
    {
        private const string defaultAzureCloudMetadataUrl = "https://management.azure.com/metadata/endpoints?api-version=2023-11-01";

        [JsonPropertyName("portal")]
        public string? PortalUrl { get; set; }

        [JsonPropertyName("authentication")]
        public AuthenticationDetails? Authentication { get; set; }

        [JsonPropertyName("media")]
        public string? MediaUrl { get; set; }

        [JsonPropertyName("graphAudience")]
        public string? GraphAudienceUrl { get; set; }

        [JsonPropertyName("graph")]
        public string? GraphUrl { get; set; }

        [JsonPropertyName("name")]
        public string? Name { get; set; }

        [JsonPropertyName("suffixes")]
        public EndpointSuffixes? Suffixes { get; set; }

        [JsonPropertyName("batch")]
        public string? BatchUrl { get; set; }

        [JsonPropertyName("resourceManager")]
        public string? ResourceManagerUrl { get; set; }

        [JsonPropertyName("vmImageAliasDoc")]
        public string? VmImageAliasDocumentationUrl { get; set; }

        [JsonPropertyName("sqlManagement")]
        public string? SqlManagementUrl { get; set; }

        [JsonPropertyName("microsoftGraphResourceId")]
        public string? MicrosoftGraphResourceUrl { get; set; }

        [JsonPropertyName("appInsightsResourceId")]
        public string? ApplicationInsightsResourceUrl { get; set; }

        [JsonPropertyName("appInsightsTelemetryChannelResourceId")]
        public string? ApplicationInsightsTelemetryChannelResourceUrl { get; set; }

        [JsonPropertyName("synapseAnalyticsResourceId")]
        public string? SynapseAnalyticsResourceUrl { get; set; }

        [JsonPropertyName("logAnalyticsResourceId")]
        public string? LogAnalyticsResourceUrl { get; set; }

        [JsonPropertyName("ossrDbmsResourceId")]
        public string? OssrDbmsResourceUrl { get; set; }

        public static async Task<AzureCloudConfig> CreateAsync(string azureCloudMetadataUrl = defaultAzureCloudMetadataUrl)
        {
            // It's critical that this succeeds for TES to function
            // Retry every minute for up to 10 minutes
            // These URLs are expected to always be available
            var retryPolicy = Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(10, retryAttempt => TimeSpan.FromMinutes(1), onRetry: (outcome, timespan, retryAttempt, context) =>
                {
                    Console.WriteLine(context.CorrelationId);
                });

            using var httpClient = new HttpClient();

            return await retryPolicy.ExecuteAsync(async () =>
            {
                var httpResponse = await httpClient.GetAsync(azureCloudMetadataUrl);
                httpResponse.EnsureSuccessStatusCode();
                var jsonString = await httpResponse.Content.ReadAsStringAsync();
                var config = JsonSerializer.Deserialize<AzureCloudConfig>(jsonString)!;
                return config;
            });
        }
    }

    public class AuthenticationDetails
    {
        [JsonPropertyName("loginEndpoint")]
        public string LoginEndpointUrl { get; set; } = "https://login.microsoftonline.com";

        [JsonPropertyName("audiences")]
        public List<string>? Audiences { get; set; }

        [JsonPropertyName("tenant")]
        public string? Tenant { get; set; }

        [JsonPropertyName("identityProvider")]
        public string? IdentityProvider { get; set; }
    }

    public class EndpointSuffixes
    {
        [JsonPropertyName("acrLoginServer")]
        public string? AcrLoginServerSuffix { get; set; }

        [JsonPropertyName("sqlServerHostname")]
        public string? SqlServerHostnameSuffix { get; set; }

        [JsonPropertyName("keyVaultDns")]
        public string? KeyVaultDnsSuffix { get; set; }

        [JsonPropertyName("storage")]
        public string? StorageSuffix { get; set; }

        [JsonPropertyName("storageSyncEndpointSuffix")]
        public string? StorageSyncEndpointSuffix { get; set; }

        [JsonPropertyName("mhsmDns")]
        public string? ManagedHsmDnsSuffix { get; set; }

        [JsonPropertyName("mysqlServerEndpoint")]
        public string? MysqlServerEndpointSuffix { get; set; }

        [JsonPropertyName("postgresqlServerEndpoint")]
        public string? PostgresqlServerEndpointSuffix { get; set; }

        [JsonPropertyName("mariadbServerEndpoint")]
        public string? MariadbServerEndpointSuffix { get; set; }

        [JsonPropertyName("synapseAnalytics")]
        public string? SynapseAnalyticsSuffix { get; set; }
    }
}

