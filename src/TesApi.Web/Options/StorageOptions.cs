// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Options
{
    /// <summary>
    /// Default storage account configuration options
    /// </summary>
    public class StorageOptions
    {
        /// <summary>
        /// Default storage account configuration section
        /// </summary>
        public const string SectionName = "Storage";

        /// <summary>
        /// External storage container information
        /// </summary>
        public string ExternalStorageContainers { get; set; }
        /// <summary>
        /// Default storage account name.
        /// </summary>
        public string DefaultAccountName { get; set; } = string.Empty;
        /// <summary>
        /// Container name for storing task information.
        /// </summary>
        public string ExecutionsContainerName { get; set; }
        // TODO: add additional properties to run without a managed id?
    }
}
