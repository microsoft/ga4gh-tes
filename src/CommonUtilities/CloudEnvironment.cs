// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Identity;
using Azure.ResourceManager;

namespace CommonUtilities
{
    /// <summary>
    /// Gets minimum cloud authentication metadata for resource management
    /// <paramref name="ArmEnvironment">Gets <see cref="ArmEnvironment"/>.</paramref>
    /// <paramref name="AzureAuthorityHost">The default host of the Microsoft Entra authority for tenants in the Azure Cloud.</paramref>
    /// </summary>
    public record struct CloudEnvironment(ArmEnvironment ArmEnvironment, Uri AzureAuthorityHost)
    {
        /// <summary>
        /// Gets base URI of the management API endpoint.
        /// </summary>
        public readonly Uri Endpoint => ArmEnvironment.Endpoint;

        /// <summary>
        /// Gets authentication audience.
        /// </summary>
        public readonly string Audience => ArmEnvironment.Audience;

        /// <summary>
        /// Gets default authentication scope.
        /// </summary>
        public readonly string DefaultScope => ArmEnvironment.DefaultScope;

        private static CloudEnvironment FromLibraries(ArmEnvironment armEnvironment, Uri azureAuthorityHost)
        {
            ArgumentNullException.ThrowIfNull(armEnvironment);
            ArgumentNullException.ThrowIfNull(azureAuthorityHost);

            return new(armEnvironment, azureAuthorityHost);
        }

        private static CloudEnvironment FromLibraries(string? name)
             => name switch
             {
                 nameof(AzureAuthorityHosts.AzurePublicCloud) => FromLibraries(ArmEnvironment.AzurePublicCloud, AzureAuthorityHosts.AzurePublicCloud),
                 nameof(AzureAuthorityHosts.AzureGovernment) => FromLibraries(ArmEnvironment.AzureGovernment, AzureAuthorityHosts.AzureGovernment),
                 nameof(AzureAuthorityHosts.AzureChina) => FromLibraries(ArmEnvironment.AzureChina, AzureAuthorityHosts.AzureChina),
                 null => FromLibraries(ArmEnvironment.AzurePublicCloud, AzureAuthorityHosts.AzurePublicCloud),
                 _ => throw new InvalidOperationException("Unknown cloud."),
             };

        public static CloudEnvironment GetCloud(string? cloudName) =>
            cloudName?.ToLowerInvariant() switch
            {
                "azurepubliccloud" => FromLibraries(nameof(AzureAuthorityHosts.AzurePublicCloud)),
                "azurecloud" => FromLibraries(nameof(AzureAuthorityHosts.AzurePublicCloud)),
                "azureusgovernmentcloud" => FromLibraries(nameof(AzureAuthorityHosts.AzureGovernment)),
                "azureusgovernment" => FromLibraries(nameof(AzureAuthorityHosts.AzureGovernment)),
                "azurechinacloud" => FromLibraries(nameof(AzureAuthorityHosts.AzureChina)),
                null => throw new ArgumentNullException(nameof(cloudName)),
                _ => throw new ArgumentOutOfRangeException(nameof(cloudName)),
            };
    }
}
