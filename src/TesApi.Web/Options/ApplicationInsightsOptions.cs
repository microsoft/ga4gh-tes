// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Options
{
    /// <summary>
    /// Default application insights configuration options
    /// </summary>
    public class ApplicationInsightsOptions
    {
        /// <summary>
        /// Configuration section.
        /// </summary>
        public const string SectionName = "ApplicationInsights";

        /// <summary>
        /// Account name.
        /// </summary>
        public string AccountName { get; set; }

        /// <summary>
        /// Connection string.
        /// </summary>
        public string ConnectionString { get; set; }
        // TODO: add additional properties to run without a managed id?
    }
}
