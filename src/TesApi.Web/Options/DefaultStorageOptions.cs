// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Options
{
    /// <summary>
    /// Default storage account configuration options
    /// </summary>
    public class DefaultStorageOptions
    {
        /// <summary>
        /// Default storage account configuration section
        /// </summary>
        public const string SectionName = "DefaultStorage";

        /// <summary>
        /// Account name.
        /// </summary>
        public string AccountName { get; set; }
        // TODO: add additional properties to run without a managed id?
    }
}
