﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Options
{
    /// <summary>
    /// Batch compute node configuration options
    /// </summary>
    public class BatchNodesOptions
    {
        /// <summary>
        /// Batch compute node configuration section
        /// </summary>
        public const string SectionName = "BatchNodes";

        /// <summary>
        /// True to disable providing compute nodes a public ip address, False otherwise
        /// </summary>
        public bool DisablePublicIpAddress { get; set; } = false;
        /// <summary>
        /// Full resource id to the subnet all batch nodes will be attached to
        /// </summary>
        public string SubnetId { get; set; } = string.Empty;
        /// <summary>
        /// Full resource id to the global managed identity
        /// </summary>
        public string GlobalManagedIdentity { get; set; } = string.Empty;
        /// <summary>
        /// Path to the global start task script
        /// </summary>
        public string GlobalStartTask { get; set; } = string.Empty;
        /// <summary>
        /// True to enable advanced VM performance monitoring, False otherwise
        /// </summary>
        public bool AdvancedVmPerformanceMonitoringEnabled { get; set; } = false;
    }
}
