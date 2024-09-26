// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Options
{
    /// <summary>
    /// Default application insights configuration options
    /// </summary>
    /// <remarks>If <see cref="ConnectionString"/> is not provided and <see cref="AccountName"/> is provided, a management plane api call will be made to obtain the connection string.</remarks>
    public class ApplicationInsightsOptions
    {
        /// <summary>
        /// Configuration section.
        /// </summary>
        public const string SectionName = "ApplicationInsights";

        /// <summary>
        /// Account name.
        /// </summary>
        /// <remarks>If <see cref="ConnectionString"/> is provided, <see cref="AccountName"/> is ignored.</remarks>
        public string AccountName { get; set; }

        /// <summary>
        /// Connection string.
        /// </summary>
        /// <remarks>Alternative environment variable: <c>APPLICATIONINSIGHTS_CONNECTION_STRING</c></remarks>
        public string ConnectionString { get; set; }
    }
}
