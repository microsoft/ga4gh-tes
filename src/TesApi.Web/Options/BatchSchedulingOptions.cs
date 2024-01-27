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

        /// <summary>
        /// Default value for <see cref="PoolRotationForcedDays"/>
        /// </summary>
        public const double DefaultPoolRotationForcedDays = 30.0;

        /// <summary>
        /// Pool Id prefix and metadata value used to associate batch account pools and jobs to this scheduler
        /// </summary>
        /// <remarks>
        /// This value must be provided.
        /// </remarks>
        public string Prefix { get; set; }

        /// <summary>
        /// Maximum active lifetime of an azure batch pool
        /// </summary>
        public double PoolRotationForcedDays { get; set; } = DefaultPoolRotationForcedDays;

        /// <summary>
        /// Disable scheduling of dedicated compute nodes
        /// </summary>
        public bool UsePreemptibleVmsOnly { get; set; } = false;
    }
}
