// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using Azure.Identity;

namespace TesApi.Web.Options
{
    public class GeneralOptions
    {
        public const string SectionName = "Tes";
        public Uri AzureAuthorityHost { get; set; } = AzureAuthorityHosts.AzurePublicCloud;
    }
}
