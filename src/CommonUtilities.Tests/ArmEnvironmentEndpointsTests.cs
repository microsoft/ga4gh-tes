// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Frozen;

namespace CommonUtilities.Tests
{
    [TestClass]
    public class ArmEnvironmentEndpointsTests
    {
        public enum Cloud
        {
            Public,
            USGovernment,
            China
        }

        private static readonly FrozenDictionary<Cloud, FrozenDictionary<string, (Type Type, object Value)>> CloudEndpoints
            = new Dictionary<Cloud, FrozenDictionary<string, (Type Type, object Value)>>()
        {
            { Cloud.Public, new Dictionary<string, (Type Type, object Value)>(StringComparer.Ordinal) {
                { nameof(ArmEnvironmentEndpoints.AuthorityHost), (typeof(Uri), new Uri("https://login.microsoftonline.com")) },
                { nameof(ArmEnvironmentEndpoints.Audience), (typeof(string), "https://management.azure.com/") },
                { nameof(ArmEnvironmentEndpoints.Tenant), (typeof(string), "common") },
                { nameof(ArmEnvironmentEndpoints.ResourceManager), (typeof(Uri), new Uri("https://management.azure.com/")) },
                { nameof(ArmEnvironmentEndpoints.AppInsightsResource), (typeof(Uri), new Uri("https://api.applicationinsights.io")) },
                { nameof(ArmEnvironmentEndpoints.AppInsightsTelemetry), (typeof(Uri), new Uri("https://dc.applicationinsights.azure.com/v2/track")) },
                { nameof(ArmEnvironmentEndpoints.BatchResource), (typeof(Uri), new Uri("https://batch.core.windows.net/")) },
                { nameof(ArmEnvironmentEndpoints.AcrSuffix), (typeof(string), "azurecr.io") },
                { nameof(ArmEnvironmentEndpoints.KeyVaultSuffix), (typeof(string), "vault.azure.net") },
                { nameof(ArmEnvironmentEndpoints.StorageSuffix), (typeof(string), "core.windows.net") },
                { nameof(ArmEnvironmentEndpoints.PostgresqlSuffix), (typeof(string), "postgres.database.azure.com") },
            }.ToFrozenDictionary() },

            { Cloud.USGovernment, new Dictionary<string, (Type Type, object Value)>(StringComparer.Ordinal) {
                { nameof(ArmEnvironmentEndpoints.AuthorityHost), (typeof(Uri), new Uri("https://login.microsoftonline.us")) },
                { nameof(ArmEnvironmentEndpoints.Audience), (typeof(string), "https://management.usgovcloudapi.net") },
                { nameof(ArmEnvironmentEndpoints.Tenant), (typeof(string), "common") },
                { nameof(ArmEnvironmentEndpoints.ResourceManager), (typeof(Uri), new Uri("https://management.usgovcloudapi.net/")) },
                { nameof(ArmEnvironmentEndpoints.AppInsightsResource), (typeof(Uri), new Uri("https://api.applicationinsights.us")) },
                { nameof(ArmEnvironmentEndpoints.AppInsightsTelemetry), (typeof(Uri), new Uri("https://dc.applicationinsights.us/v2/track")) },
                { nameof(ArmEnvironmentEndpoints.BatchResource), (typeof(Uri), new Uri("https://batch.core.usgovcloudapi.net/")) },
                { nameof(ArmEnvironmentEndpoints.AcrSuffix), (typeof(string), "azurecr.us") },
                { nameof(ArmEnvironmentEndpoints.KeyVaultSuffix), (typeof(string), "vault.usgovcloudapi.net") },
                { nameof(ArmEnvironmentEndpoints.StorageSuffix), (typeof(string), "core.usgovcloudapi.net") },
                { nameof(ArmEnvironmentEndpoints.PostgresqlSuffix), (typeof(string), "postgres.database.usgovcloudapi.net") },
            }.ToFrozenDictionary() },

            { Cloud.China, new Dictionary<string, (Type Type, object Value)>(StringComparer.Ordinal) {
                { nameof(ArmEnvironmentEndpoints.AuthorityHost), (typeof(Uri), new Uri("https://login.chinacloudapi.cn")) },
                { nameof(ArmEnvironmentEndpoints.Audience), (typeof(string), "https://management.chinacloudapi.cn") },
                { nameof(ArmEnvironmentEndpoints.Tenant), (typeof(string), "common") },
                { nameof(ArmEnvironmentEndpoints.ResourceManager), (typeof(Uri), new Uri("https://management.chinacloudapi.cn/")) },
                { nameof(ArmEnvironmentEndpoints.AppInsightsResource), (typeof(Uri), new Uri("https://api.applicationinsights.azure.cn")) },
                { nameof(ArmEnvironmentEndpoints.AppInsightsTelemetry), (typeof(Uri), new Uri("https://dc.applicationinsights.azure.cn/v2/track")) },
                { nameof(ArmEnvironmentEndpoints.BatchResource), (typeof(Uri), new Uri("https://batch.chinacloudapi.cn/")) },
                { nameof(ArmEnvironmentEndpoints.AcrSuffix), (typeof(string), "azurecr.cn") },
                { nameof(ArmEnvironmentEndpoints.KeyVaultSuffix), (typeof(string), "vault.azure.cn") },
                { nameof(ArmEnvironmentEndpoints.StorageSuffix), (typeof(string), "core.chinacloudapi.cn") },
                { nameof(ArmEnvironmentEndpoints.PostgresqlSuffix), (typeof(string), "postgres.database.chinacloudapi.cn") },
            }.ToFrozenDictionary() },
        }.ToFrozenDictionary();

        [DataTestMethod]
        [DataRow("AzureCloud", "https://management.azure.com/", DisplayName = "AzureCloud")]
        [DataRow("AzurePublicCloud", "https://management.azure.com/", DisplayName = "AzurePublicCloud")]
        [DataRow("AzureUSGovernment", "https://management.usgovcloudapi.net", DisplayName = "AzureUSGovernment")]
        [DataRow("AzureUSGovernmentCloud", "https://management.usgovcloudapi.net", DisplayName = "AzureUSGovernmentCloud")]
        [DataRow("AzureChinaCloud", "https://management.chinacloudapi.cn", DisplayName = "AzureChinaCloud")]
        public async Task FromKnownCloudNameAsync_ExpectedEndpoints(string cloud, string audience)
        {
            var environment = await ArmEnvironmentEndpoints.FromKnownCloudNameAsync(cloud, Microsoft.Extensions.Options.Options.Create(new Options.RetryPolicyOptions()));
            Assert.AreEqual(audience, GetPropertyFromEnvironment<string>(environment, nameof(ArmEnvironmentEndpoints.Audience)));
        }

        private static T? GetPropertyFromEnvironment<T>(ArmEnvironmentEndpoints environment, string property)
        {
            return (T?)environment.GetType().GetProperty(property)?.GetValue(environment);
        }

        [DataTestMethod]
        [DataRow(Cloud.Public, "AzureCloud", DisplayName = "All generally available global Azure regions")]
        [DataRow(Cloud.USGovernment, "AzureUSGovernment", DisplayName = "Azure Government")]
        [DataRow(Cloud.China, "AzureChinaCloud", DisplayName = "Microsoft Azure operated by 21Vianet")]
        public async Task FromKnownCloudNameAsync_ExpectedValues(Cloud cloud, string cloudName)
        {
            var environment = await ArmEnvironmentEndpoints.FromKnownCloudNameAsync(cloudName, Microsoft.Extensions.Options.Options.Create(new Options.RetryPolicyOptions()));
            foreach (var (property, metadata) in CloudEndpoints[cloud])
            {
                switch (metadata.Type)
                {
                    case var x when typeof(string).Equals(x):
                        Assert.AreEqual(metadata.Value, GetPropertyFromEnvironment<string>(environment, property));
                        break;
                    case var x when typeof(Uri).Equals(x):
                        Assert.AreEqual(metadata.Value, GetPropertyFromEnvironment<Uri>(environment, property));
                        break;
                    default:
                        throw new NotSupportedException();
                }
            }
        }
    }
}
