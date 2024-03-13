// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Options
{
    /// <summary>
    /// General TES system options
    /// </summary>
    public class GeneralOptions
    {
        /// <summary>
        /// General options configuration section name
        /// </summary>
        public const string SectionName = "General";

        /// <summary>
        /// Azure cloud name.  Defined here: https://github.com/Azure/azure-sdk-for-net/blob/bc9f38eca0d8abbf0697dd3e3e75220553eeeafa/sdk/identity/Azure.Identity/src/AzureAuthorityHosts.cs#L11
        /// </summary>
        public string AzureCloudName { get; set; } = "AzureCloud"; // or "AzureUSGovernment"

        /// <summary>
        /// Version of the API to use to get the Azure cloud metadata
        /// </summary>
        public string AzureCloudMetadataUrlApiVersion { get; set; } = "2023-11-01";
    }
}
