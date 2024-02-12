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
        /// URL to obtain Azure cloud metadata at runtime
        /// </summary>
        public string AzureCloudManagementUrl { get; set; } = "https://management.azure.com/metadata/endpoints?api-version=2023-11-01";
    }
}
