// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using CommonUtilities.AzureCloud;

namespace CommonUtilities
{
    public record class AzureEnvironmentConfig(string? AzureAuthorityHostUrl, string? TokenScope, string? StorageUrlSuffix)
    {
        public static AzureEnvironmentConfig FromArmEnvironmentEndpoints(AzureCloudConfig azureCloudConfig)
        {
            ArgumentNullException.ThrowIfNull(azureCloudConfig);
            return new(azureCloudConfig.Authentication?.LoginEndpointUrl, azureCloudConfig.DefaultTokenScope, azureCloudConfig.Suffixes?.StorageSuffix);
        }
    }
}
