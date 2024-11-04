// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Frozen;
using CommonUtilities.AzureCloud;

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

        private struct DictionaryValueEqualityComparer<T> : IEqualityComparer<object>
        {
            readonly bool IEqualityComparer<object>.Equals(object? x, object? y)
            {
                return x switch
                {
                    IReadOnlyDictionary<string, object?> X => y switch
                    {
                        T Y => DictionaryValueEqualityComparer<T>.Equals(X, Y),
                        _ => false,
                    },
                    null => y is null,
                    _ => x.Equals(y),
                };
            }

            private static bool Equals(IReadOnlyDictionary<string, object?> x, T y)
            {
                var actual = FrozenDictionary(x.Keys.Select(name => (Name: name, Property: typeof(T).GetProperty(name))).Where(e => e.Property is not null).Select(e => (e.Name, e.Property!.GetValue(y))));
                if (x.Count != actual.Count) return false;
                if (x.Keys.Order(StringComparer.Ordinal).Zip(actual.Keys.Order(StringComparer.Ordinal)).Any(e => !e.First.Equals(e.Second, StringComparison.Ordinal))) return false;

                foreach (var key in x.Keys)
                {
                    switch (x[key])
                    {
                        case IReadOnlyDictionary<string, object?> expected:
                            if (!ArmEnvironmentEndpointsTests.Equals(expected, actual[key])) return false;
                            break;

                        default:
                            if (!Equals(x[key], actual[key])) return false;
                            break;
                    }
                }

                return true;
            }

            readonly int IEqualityComparer<object>.GetHashCode(object obj) => obj.GetHashCode();
        }

        private static bool Equals(IReadOnlyDictionary<string, object?> x, object? y) => y switch
        {
            AzureCloudConfig.AuthenticationDetails z => ((IEqualityComparer<object>)new DictionaryValueEqualityComparer<AzureCloudConfig.AuthenticationDetails>()).Equals(x, z),
            AzureCloudConfig.EndpointSuffixes z => ((IEqualityComparer<object>)new DictionaryValueEqualityComparer<AzureCloudConfig.EndpointSuffixes>()).Equals(x, z),
            _ => false,
        };

        private static FrozenDictionary<string, object?> FrozenDictionary(IEnumerable<(string, object?)> values)
        {
            var dictionary = new Dictionary<string, object?>(StringComparer.Ordinal);
            dictionary.AddRange(values.ToDictionary());
            return dictionary.ToFrozenDictionary();
        }

        private static readonly FrozenDictionary<Cloud, FrozenDictionary<string, object?>> CloudEndpoints
            = new Dictionary<Cloud, FrozenDictionary<string, object?>>()
        {
            { Cloud.Public, FrozenDictionary(
                [
                    (nameof(AzureCloudConfig.Authentication), FrozenDictionary(
                    [
                        (nameof(AzureCloudConfig.AuthenticationDetails.LoginEndpointUrl), "https://login.microsoftonline.com"),
                        (nameof(AzureCloudConfig.AuthenticationDetails.Tenant), "common"),
                    ])),
                    (nameof(AzureCloudConfig.ResourceManagerUrl), "https://management.azure.com/"),
                    (nameof(AzureCloudConfig.ApplicationInsightsResourceUrl), "https://api.applicationinsights.io"),
                    (nameof(AzureCloudConfig.ApplicationInsightsTelemetryChannelResourceUrl), "https://dc.applicationinsights.azure.com/v2/track"),
                    (nameof(AzureCloudConfig.BatchUrl), "https://batch.core.windows.net/"),
                    (nameof(AzureCloudConfig.Suffixes), FrozenDictionary(
                    [
                        (nameof(AzureCloudConfig.Suffixes.AcrLoginServerSuffix), "azurecr.io"),
                        (nameof(AzureCloudConfig.Suffixes.KeyVaultDnsSuffix), "vault.azure.net"),
                        (nameof(AzureCloudConfig.Suffixes.StorageSuffix), "core.windows.net"),
                        (nameof(AzureCloudConfig.Suffixes.PostgresqlServerEndpointSuffix), "postgres.database.azure.com"),
                    ])),
                ]
            )},

            { Cloud.USGovernment, FrozenDictionary(
                [
                    (nameof(AzureCloudConfig.Authentication), FrozenDictionary(
                    [
                        (nameof(AzureCloudConfig.AuthenticationDetails.LoginEndpointUrl), "https://login.microsoftonline.us"),
                        (nameof(AzureCloudConfig.AuthenticationDetails.Tenant), "common"),
                    ])),
                    (nameof(AzureCloudConfig.ResourceManagerUrl), "https://management.usgovcloudapi.net"),
                    (nameof(AzureCloudConfig.ApplicationInsightsResourceUrl), "https://api.applicationinsights.us"),
                    (nameof(AzureCloudConfig.ApplicationInsightsTelemetryChannelResourceUrl), "https://dc.applicationinsights.us/v2/track"),
                    (nameof(AzureCloudConfig.BatchUrl), "https://batch.core.usgovcloudapi.net"),
                    (nameof(AzureCloudConfig.Suffixes), FrozenDictionary(
                    [
                        (nameof(AzureCloudConfig.Suffixes.AcrLoginServerSuffix), "azurecr.us"),
                        (nameof(AzureCloudConfig.Suffixes.KeyVaultDnsSuffix), "vault.usgovcloudapi.net"),
                        (nameof(AzureCloudConfig.Suffixes.StorageSuffix), "core.usgovcloudapi.net"),
                        (nameof(AzureCloudConfig.Suffixes.PostgresqlServerEndpointSuffix), "postgres.database.usgovcloudapi.net"),
                    ])),
                ]
            )},

            { Cloud.China, FrozenDictionary(
                [
                    (nameof(AzureCloudConfig.Authentication), FrozenDictionary(
                    [
                        (nameof(AzureCloudConfig.AuthenticationDetails.LoginEndpointUrl), "https://login.chinacloudapi.cn"),
                        (nameof(AzureCloudConfig.AuthenticationDetails.Tenant), "common"),
                    ])),
                    (nameof(AzureCloudConfig.ResourceManagerUrl), "https://management.chinacloudapi.cn"),
                    (nameof(AzureCloudConfig.ApplicationInsightsResourceUrl), "https://api.applicationinsights.azure.cn"),
                    (nameof(AzureCloudConfig.ApplicationInsightsTelemetryChannelResourceUrl), "https://dc.applicationinsights.azure.cn/v2/track"),
                    (nameof(AzureCloudConfig.BatchUrl), "https://batch.chinacloudapi.cn"),
                    (nameof(AzureCloudConfig.Suffixes), FrozenDictionary(
                    [
                        (nameof(AzureCloudConfig.Suffixes.AcrLoginServerSuffix), "azurecr.cn"),
                        (nameof(AzureCloudConfig.Suffixes.KeyVaultDnsSuffix), "vault.azure.cn"),
                        (nameof(AzureCloudConfig.Suffixes.StorageSuffix), "core.chinacloudapi.cn"),
                        (nameof(AzureCloudConfig.Suffixes.PostgresqlServerEndpointSuffix), "postgres.database.chinacloudapi.cn"),
                    ])),
                ]
            )},
        }.ToFrozenDictionary();

        [DataTestMethod]
        [Ignore]
        [DataRow("AzureCloud", "https://management.azure.com//.default", DisplayName = "AzureCloud")]
        [DataRow("AzurePublicCloud", "https://management.azure.com//.default", DisplayName = "AzurePublicCloud")]
        [DataRow("AzureUSGovernment", "https://management.usgovcloudapi.net/.default", DisplayName = "AzureUSGovernment")]
        [DataRow("AzureUSGovernmentCloud", "https://management.usgovcloudapi.net/.default", DisplayName = "AzureUSGovernmentCloud")]
        [DataRow("AzureChinaCloud", "https://management.chinacloudapi.cn/.default", DisplayName = "AzureChinaCloud")]
        public async Task FromKnownCloudNameAsync_ExpectedDefaultTokenScope(string cloud, string audience)
        {
            var environment = await AzureCloudConfig.FromKnownCloudNameAsync(cloudName: cloud, retryPolicyOptions: Microsoft.Extensions.Options.Options.Create(new Options.RetryPolicyOptions()));
            Assert.AreEqual(audience, GetPropertyFromEnvironment<string>(environment, nameof(AzureCloudConfig.DefaultTokenScope)));
        }

        private static T? GetPropertyFromEnvironment<T>(AzureCloudConfig environment, string property)
        {
            return (T?)environment.GetType().GetProperty(property)?.GetValue(environment);
        }

        [DataTestMethod]
        [Ignore]
        [DataRow(Cloud.Public, "AzureCloud", DisplayName = "All generally available global Azure regions")]
        [DataRow(Cloud.USGovernment, "AzureUSGovernment", DisplayName = "Azure Government")]
        [DataRow(Cloud.China, "AzureChinaCloud", DisplayName = "Microsoft Azure operated by 21Vianet")]
        public async Task FromKnownCloudNameAsync_ExpectedValues(Cloud cloud, string cloudName)
        {
            var environment = await AzureCloudConfig.FromKnownCloudNameAsync(cloudName: cloudName, retryPolicyOptions: Microsoft.Extensions.Options.Options.Create(new Options.RetryPolicyOptions()));
            foreach (var (property, value) in CloudEndpoints[cloud])
            {
                switch (value)
                {
                    case var x when x is string:
                        Assert.AreEqual(value, GetPropertyFromEnvironment<string>(environment, property));
                        break;

                    case var x when x is IReadOnlyDictionary<string, object>:
                        switch (property)
                        {
                            case nameof(AzureCloudConfig.Authentication):
                                Assert.AreEqual(value, GetPropertyFromEnvironment<AzureCloudConfig.AuthenticationDetails>(environment, property), new DictionaryValueEqualityComparer<AzureCloudConfig.AuthenticationDetails>());
                                break;

                            case nameof(AzureCloudConfig.Suffixes):
                                Assert.AreEqual(value, GetPropertyFromEnvironment<AzureCloudConfig.EndpointSuffixes>(environment, property), new DictionaryValueEqualityComparer<AzureCloudConfig.EndpointSuffixes>());
                                break;
                        }
                        break;

                    default:
                        throw new NotSupportedException();
                }
            }
        }
    }
}
