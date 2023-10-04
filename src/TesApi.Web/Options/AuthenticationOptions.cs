// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Options
{
    public class AuthenticationOptions
    {
        public const string SectionName = "Authentication";
        public AuthenticationProviderOptions[] Providers { get; set; }
    }
}
