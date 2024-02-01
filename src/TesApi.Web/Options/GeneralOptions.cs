// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Options
{
    public class GeneralOptions
    {
        public const string SectionName = "General";
        public string AzureCloudName { get; set; } = "AzureCloud";
        public string AzureCloudManagementUrl { get; set; } = "https://management.azure.com/metadata/endpoints?api-version=2023-11-01";
    }
}
