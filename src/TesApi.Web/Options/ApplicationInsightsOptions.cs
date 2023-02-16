namespace TesApi.Web.Options
{
    // Copyright (c) Microsoft Corporation.
    // Licensed under the MIT License.

    /// <summary>
    /// Application insights account configuration options
    /// </summary>
    public class ApplicationInsightsOptions
    {
        /// <summary>
        /// Application insights account configuration section
        /// </summary>
        public const string SectionName = "ApplicationInsights";

        /// <summary>
        /// Account name.
        /// </summary>
        public string AccountName { get; set; }
        // TODO: add additional properties to run without a managed id?
    }
}
