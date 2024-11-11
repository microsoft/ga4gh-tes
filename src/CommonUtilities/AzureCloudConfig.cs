// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json;
using System.Text.Json.Serialization;
using Azure.Identity;
using Azure.ResourceManager;
using Microsoft.Extensions.Options;

namespace CommonUtilities.AzureCloud
{
    public class AzureCloudConfig
    {
        private const string DefaultAzureCloudMetadataUrlApiVersion = "2023-11-01";
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

        // Critical properties here

        public string? DefaultTokenScope { get; set; }
        public ArmEnvironment? ArmEnvironment { get; set; }
        public string? Domain { get; set; }
        public AzureEnvironmentConfig? AzureEnvironmentConfig { get; set; }

        // Helper properties

        public Uri? AuthorityHost { get; set; }

        /// <summary>
        /// Azure cloud endpoints from cloud name
        /// </summary>
        /// <param name="cloudName">Name of Azure cloud, either from IMDS or the resource manager metadata/endpoints query.</param>
        /// <returns><see cref="AzureCloudConfig"/>.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        /// <remarks>
        /// Recognized cloud names are:
        /// <para>All generally available global Azure regions: <c>AzureCloud</c>, <c>AzurePublicCloud</c></para>
        /// <para>Azure Government: <c>AzureUSGovernment</c>: <c>AzureUSGovernmentCloud</c></para>
        /// <para>Microsoft Azure operated by 21Vianet: <c>AzureChinaCloud</c></para>
        /// </remarks>
        public static Task<AzureCloudConfig> FromKnownCloudNameAsync(string cloudName = DefaultAzureCloudName, string azureCloudMetadataUrlApiVersion = DefaultAzureCloudMetadataUrlApiVersion, IOptions<Options.RetryPolicyOptions>? retryPolicyOptions = default)
        {
            return cloudName.ToLowerInvariant() switch
            {
                "azurecloud" => FromMetadataEndpointsAsync(AzurePublicCloud, azureCloudMetadataUrlApiVersion, retryPolicyOptions),
                "azurepubliccloud" => FromMetadataEndpointsAsync(AzurePublicCloud, azureCloudMetadataUrlApiVersion, retryPolicyOptions),
                "azureusgovernmentcloud" => FromMetadataEndpointsAsync(AzureUSGovernmentCloud, azureCloudMetadataUrlApiVersion, retryPolicyOptions),
                "azureusgovernment" => FromMetadataEndpointsAsync(AzureUSGovernmentCloud, azureCloudMetadataUrlApiVersion, retryPolicyOptions),
                "azurechinacloud" => FromMetadataEndpointsAsync(AzureChinaCloud, azureCloudMetadataUrlApiVersion, retryPolicyOptions),
                null => throw new ArgumentNullException(nameof(cloudName)),
                _ => throw new ArgumentOutOfRangeException(nameof(cloudName)),
            };
        }

        public static AzureCloudConfig ForUnitTesting()
        {
            return new()
            {
                PortalUrl = "http://portal.test",
                Authentication = new() { Tenant = "common" },
                MediaUrl = "http://media.test",
                GraphAudienceUrl = "http://graph.test",
                Name = "Test",
                Suffixes = new()
                {
                    AcrLoginServerSuffix = "azurecr.io",
                    KeyVaultDnsSuffix = "vault.azure.net",
                    StorageSuffix = "core.windows.net",
                    PostgresqlServerEndpointSuffix = "postgres.database.azure.com"
                },
                BatchUrl = "https://batch.core.windows.net/",
                ResourceManagerUrl = "https://management.azure.com/",
                MicrosoftGraphResourceUrl = "http://graph.test",
                ApplicationInsightsResourceUrl = "https://api.applicationinsights.io",
                ApplicationInsightsTelemetryChannelResourceUrl = "https://dc.applicationinsights.azure.com/v2/track",

                DefaultTokenScope = "https://management.azure.com//.default",
                ArmEnvironment = new(new("https://management.azure.com/"), "https://management.azure.com/"),
                Domain = "Test",
                AzureEnvironmentConfig = new("https://login.microsoftonline.com", "https://management.azure.com//.default", "core.windows.net")
            };
        }

        private static readonly Uri AzurePublicCloud = Azure.ResourceManager.ArmEnvironment.AzurePublicCloud.Endpoint;
        private static readonly Uri AzureUSGovernmentCloud = Azure.ResourceManager.ArmEnvironment.AzureGovernment.Endpoint;
        private static readonly Uri AzureChinaCloud = Azure.ResourceManager.ArmEnvironment.AzureChina.Endpoint;

        /// <summary>
        /// Azure cloud endpoints from cloud management endpoints
        /// </summary>
        /// <param name="cloudManagement">Azure cloud resource management endpoint.</param>
        /// <returns></returns>
        public static async Task<AzureCloudConfig> FromMetadataEndpointsAsync(Uri cloudManagement, string azureCloudMetadataUrlApiVersion = DefaultAzureCloudMetadataUrlApiVersion, IOptions<Options.RetryPolicyOptions>? retryPolicyOptions = default)
        {
            ArgumentNullException.ThrowIfNull(cloudManagement);
            var retryPolicy = new RetryPolicyBuilder(retryPolicyOptions ?? Microsoft.Extensions.Options.Options.Create<Options.RetryPolicyOptions>(new())).DefaultRetryHttpResponseMessagePolicyBuilder().SetOnRetryBehavior().AsyncBuildPolicy();
            HttpResponseMessage response;

            {
                using HttpClient client = new();
                response = await retryPolicy.ExecuteAsync(() =>
                    client.SendAsync(new(HttpMethod.Get, new UriBuilder(cloudManagement) { Path = "/metadata/endpoints", Query = $"api-version={azureCloudMetadataUrlApiVersion}" }.Uri)));
            }

            response.EnsureSuccessStatusCode();

            var jsonString = await response.Content.ReadAsStringAsync();
            var config = JsonSerializer.Deserialize(jsonString, AzureCloudConfigContext.Default.AzureCloudConfig)!;
            config.ArmEnvironment = GetEnvironment(config.Name);
            config.DefaultTokenScope = CreateScope(config);
            config.Domain = GetDomain(config.ResourceManagerUrl);
            config.AzureEnvironmentConfig = new AzureEnvironmentConfig(config.Authentication?.LoginEndpointUrl, config.DefaultTokenScope, config.Suffixes?.StorageSuffix);
            config.AuthorityHost = GetAuthorityHost(config);
            return config;

            static string? CreateScope(AzureCloudConfig config)
            {
                if (config.ArmEnvironment is not null)
                {
                    return config.ArmEnvironment.Value.DefaultScope;
                }

                var audience = config.Authentication?.Audiences?.LastOrDefault();

                if (!string.IsNullOrWhiteSpace(audience))
                {
                    return audience + @"/.default";
                }

                return default;
            }

            static string? GetDomain(string? resourceManagerUrl)
            {
                if (!string.IsNullOrWhiteSpace(resourceManagerUrl))
                {
                    return new Uri(resourceManagerUrl).Host[@"management.".Length..];
                }

                return default;
            }

            static Uri? GetAuthorityHost(AzureCloudConfig config)
            {
                return config.Name switch
                {
                    "AzureCloud" => AzureAuthorityHosts.AzurePublicCloud,
                    "AzureUSGovernment" => AzureAuthorityHosts.AzureGovernment,
                    "AzureChinaCloud" => AzureAuthorityHosts.AzureChina,
                    // Environment.GetEnvironmentVariable("AZURE_AUTHORITY_HOST")
                    _ => GetFromConfig(),
                };

                Uri? GetFromConfig()
                {
                    var authorityHost = config.Authentication?.LoginEndpointUrl;

                    if (!string.IsNullOrWhiteSpace(authorityHost))
                    {
                        return new(authorityHost);
                    }

                    return default;
                }
            }

            static ArmEnvironment? GetEnvironment(string? name)
                => name?.ToLowerInvariant() switch
                {
                    "azurepubliccloud" => Azure.ResourceManager.ArmEnvironment.AzurePublicCloud,
                    "azurecloud" => Azure.ResourceManager.ArmEnvironment.AzurePublicCloud,
                    "azureusgovernmentcloud" => Azure.ResourceManager.ArmEnvironment.AzureGovernment,
                    "azureusgovernment" => Azure.ResourceManager.ArmEnvironment.AzureGovernment,
                    "azurechinacloud" => Azure.ResourceManager.ArmEnvironment.AzureChina,
                    _ => default,
                };
        }

        //public static async Task<AzureCloudConfig> CreateAsync(string azureCloudName = DefaultAzureCloudName, string azureCloudMetadataUrlApiVersion = DefaultAzureCloudMetadataUrlApiVersion)
        //{
        //    // It's critical that this succeeds for TES to function
        //    // These URLs are expected to always be available
        //    string domain;
        //    string defaultTokenScope;
        //    ArmEnvironment armEnvironment;
        //    // Names defined here: https://github.com/Azure/azure-sdk-for-net/blob/bc9f38eca0d8abbf0697dd3e3e75220553eeeafa/sdk/identity/Azure.Identity/src/AzureAuthorityHosts.cs#L11
        //    switch (azureCloudName.ToUpperInvariant())
        //    {
        //        case "AZURECLOUD":
        //            domain = "azure.com";
        //            // The double slash is intentional for the public cloud.
        //            // https://github.com/Azure/azure-sdk-for-net/blob/bc9f38eca0d8abbf0697dd3e3e75220553eeeafa/sdk/identity/Azure.Identity/src/AzureAuthorityHosts.cs#L53
        //            defaultTokenScope = $"https://management.{domain}//.default";
        //            armEnvironment = Azure.ResourceManager.ArmEnvironment.AzurePublicCloud;
        //            break;
        //        case "AZUREUSGOVERNMENT":
        //            domain = "usgovcloudapi.net";
        //            defaultTokenScope = $"https://management.{domain}/.default";
        //            armEnvironment = Azure.ResourceManager.ArmEnvironment.AzureGovernment;
        //            break;
        //        case "AZURECHINACLOUD":
        //            domain = "chinacloudapi.cn";
        //            defaultTokenScope = $"https://management.{domain}/.default";
        //            armEnvironment = Azure.ResourceManager.ArmEnvironment.AzureChina;
        //            break;
        //        default:
        //            throw new ArgumentException($"Invalid Azure cloud name: {azureCloudName}");
        //    }

        //    string azureCloudMetadataUrl = $"https://management.{domain}/metadata/endpoints?api-version={azureCloudMetadataUrlApiVersion}";

        //    var retryPolicy = Policy
        //        .Handle<Exception>()
        //        .WaitAndRetryAsync(10, retryAttempt => TimeSpan.FromSeconds(30), onRetry: (exception, timespan, retryAttempt, context) =>
        //        {
        //            Console.WriteLine($"Attempt {retryAttempt}: Retrying AzureCloudConfig creation due to error: {exception.Message}.  {exception}");
        //        });

        //    using var httpClient = new HttpClient();

        //    return await retryPolicy.ExecuteAsync(async () =>
        //    {
        //        var httpResponse = await httpClient.GetAsync(azureCloudMetadataUrl);
        //        httpResponse.EnsureSuccessStatusCode();
        //        var jsonString = await httpResponse.Content.ReadAsStringAsync();
        //        var config = JsonSerializer.Deserialize(jsonString, AzureCloudConfigContext.Default.AzureCloudConfig)!;
        //        config.DefaultTokenScope = defaultTokenScope;
        //        config.ArmEnvironment = armEnvironment;
        //        config.Domain = domain;
        //        config.AzureEnvironmentConfig = new AzureEnvironmentConfig(config.Authentication?.LoginEndpointUrl, config.DefaultTokenScope, config.Suffixes?.StorageSuffix);
        //        config.AuthorityHost = new(config.Authentication!.LoginEndpointUrl!);
        //        return config;
        //    });
        //}
    }

    [JsonSerializable(typeof(AzureCloudConfig))]
    public partial class AzureCloudConfigContext : JsonSerializerContext
    { }
}

