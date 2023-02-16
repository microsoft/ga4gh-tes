// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Options
{
    /// <summary>
    /// Batch scheduling configuration options
    /// </summary>
    public class BatchSchedulingOptions
    {
        /// <summary>
        /// Batch scheduling configuration section
        /// </summary>
        public const string SectionName = "BatchScheduling";

        private const double DefaultPoolRotationForcedDays = 30.0;

        /// <summary>
        /// Disable flag (this instance only services the API and only accesses the TesTask repository)
        /// </summary>
        public bool Disable { get; set; } // DisableBatchScheduling

        /// <summary>
        /// Use legacy Azure Batch Autopools implementation
        /// </summary>
        public bool UseLegacyAutopools { get; set; } // UseLegacyBatchImplementationWithAutopools

        /// <summary>
        /// Disables background service to delete Batch jobs older than seven days for completed tasks (only relevant if <see cref="UseLegacyAutopools"/> is set)
        /// </summary>
        public bool DisableJobCleanup { get; set; } // DisableBatchJobCleanup

        /// <summary>
        /// Pool Id prefix and metadata value used to associate batch account pools and jobs to this scheduler.
        /// </summary>
        public string Prefix { get; set; } // Name

        /// <summary>
        /// Maximum active lifetime of an azure batch pool
        /// </summary>
        public double PoolRotationForcedDays { get; set; } = DefaultPoolRotationForcedDays; // BatchPoolRotationForcedDays
    }
}
