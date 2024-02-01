// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using Azure.Identity;

namespace TesApi.Web.Options
{
    public class GeneralOptions
    {
        public const string SectionName = "Tes";
        public string AzureCloudName { get; set; } = "AzureCloud";
        public string AzureCloudManagementUrl { get; set; } = "https://management.azure.com/metadata/endpoints?api-version=2023-11-01";
    }
}
