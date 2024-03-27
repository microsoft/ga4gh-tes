﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Options
{
    /// <summary>
    /// Martha configuration options
    /// </summary>
    public class DrsHubOptions
    {
        /// <summary>
        /// Martha configuration section
        /// </summary>
        public const string SectionName = "DrsHub";

        /// <summary>
        /// DrsHubUrl
        /// </summary>
        public string Url { get; set; } = string.Empty;
    }
}
