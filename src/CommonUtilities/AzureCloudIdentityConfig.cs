// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Azure.Management.ResourceManager.Fluent;

namespace CommonUtilities
{
    public class AzureCloudIdentityConfig
    {
        public string? AzureAuthorityHostUrl { get; set; }
        public string? TokenScope { get; set; }
        public string? ResourceManagerUrl { get; set; }
        public AzureEnvironment AzureEnvironment { get; set; }
    }
}
