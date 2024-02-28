// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json;
using System.Text.Json.Serialization;
using Azure.ResourceManager;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Polly;

namespace CommonUtilities.AzureCloud
{
    public class AzureCloudConfig
    {
        private const string defaultAzureCloudMetadataUrlApiVersion = "2023-11-01";
        public const string DefaultAzureCloudName = "AzureCloud";

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
        public string? Name { get; set; } // AzureCloud, AzureChinaCloud, AzureUSGovernment

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

        public string? DefaultTokenScope { get; set; }
        public AzureEnvironment AzureEnvironment { get; set; }
        public ArmEnvironment ArmEnvironment { get; set; }
        public string Domain { get; set; }
        public AzureEnvironmentConfig AzureEnvironmentConfig { get; set; }

        public static async Task<AzureCloudConfig> CreateAsync(string azureCloudName = DefaultAzureCloudName, string azureCloudMetadataUrlApiVersion = defaultAzureCloudMetadataUrlApiVersion)
        {
            // It's critical that this succeeds for TES to function
            // These URLs are expected to always be available
            string domain;
            string defaultTokenScope;
            AzureEnvironment azureEnvironment;
            ArmEnvironment armEnvironment;
            // Names defined here: https://github.com/Azure/azure-sdk-for-net/blob/bc9f38eca0d8abbf0697dd3e3e75220553eeeafa/sdk/identity/Azure.Identity/src/AzureAuthorityHosts.cs#L11
            switch (azureCloudName.ToUpperInvariant())
            {
                case "AZURECLOUD":
                    domain = "azure.com";
                    // The double slash is intentional for the public cloud.
                    // https://github.com/Azure/azure-sdk-for-net/blob/bc9f38eca0d8abbf0697dd3e3e75220553eeeafa/sdk/identity/Azure.Identity/src/AzureAuthorityHosts.cs#L53
                    defaultTokenScope = $"https://management.{domain}//.default";
                    azureEnvironment = AzureEnvironment.AzureGlobalCloud;
                    armEnvironment = ArmEnvironment.AzurePublicCloud;
                    break;
                case "AZUREUSGOVERNMENT":
                    domain = "usgovcloudapi.net";
                    defaultTokenScope = $"https://management.{domain}/.default";
                    azureEnvironment = AzureEnvironment.AzureUSGovernment;
                    armEnvironment = ArmEnvironment.AzureGovernment;
                    break;
                case "AZURECHINACLOUD":
                    domain = "chinacloudapi.cn";
                    defaultTokenScope = $"https://management.{domain}/.default";
                    azureEnvironment = AzureEnvironment.AzureChinaCloud;
                    armEnvironment = ArmEnvironment.AzureChina;
                    break;
                default:
                    throw new ArgumentException($"Invalid Azure cloud name: {azureCloudName}");
            }

            string azureCloudMetadataUrl = $"https://management.{domain}/metadata/endpoints?api-version={azureCloudMetadataUrlApiVersion}";

            var retryPolicy = Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(10, retryAttempt => TimeSpan.FromSeconds(30), onRetry: (exception, timespan, retryAttempt, context) =>
                {
                    Console.WriteLine($"Attempt {retryAttempt}: Retrying AzureCloudConfig creation due to error: {exception.Message}.  {exception}");
                });

            using var httpClient = new HttpClient();

            return await retryPolicy.ExecuteAsync(async () =>
            {
                var httpResponse = await httpClient.GetAsync(azureCloudMetadataUrl);
                httpResponse.EnsureSuccessStatusCode();
                var jsonString = await httpResponse.Content.ReadAsStringAsync();
                var config = JsonSerializer.Deserialize<AzureCloudConfig>(jsonString)!;
                config.DefaultTokenScope = defaultTokenScope;
                config.AzureEnvironment = azureEnvironment;
                config.ArmEnvironment = armEnvironment;
                config.Domain = domain;

                config.AzureEnvironmentConfig = new AzureEnvironmentConfig
                {
                    AzureAuthorityHostUrl = config.Authentication?.LoginEndpointUrl,
                    TokenScope = config.DefaultTokenScope,
                    StorageUrlSuffix = config.Suffixes?.StorageSuffix,
                };

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

