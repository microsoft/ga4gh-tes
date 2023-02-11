﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;

namespace Tes.Models
{
    /// <summary>
    /// VirtualMachineInformation contains the runtime specs for a VM
    /// </summary>
    public class VirtualMachineInformation
    {
        /// <summary>
        /// Size of the VM
        /// </summary>
        [TesTaskLogMetadataKey("vm_size")]
        public string VmSize { get; set; }

        /// <summary>
        /// VM Family
        /// </summary>
        [TesTaskLogMetadataKey("vm_family")]
        public string VmFamily { get; set; }

        /// <summary>
        /// True if this is a low-pri VM, otherwise false
        /// </summary>
        [TesTaskLogMetadataKey("vm_low_priority")]
        public bool LowPriority { get; set; }

        /// <summary>
        /// VM price per hour
        /// </summary>
        [TesTaskLogMetadataKey("vm_price_per_hour_usd")]
        public decimal? PricePerHour { get; set; }

        /// <summary>
        /// VM memory size, in GB
        /// </summary>
        [TesTaskLogMetadataKey("vm_memory_in_gib")]
        public double? MemoryInGiB { get; set; }

        /// <summary>
        /// Number of cores for this VM
        /// </summary>
        [TesTaskLogMetadataKey("vm_vcpus_available")]
        public int? VCpusAvailable { get; set; }

        /// <summary>
        /// The resources disk size, in GB
        /// </summary>
        [TesTaskLogMetadataKey("vm_resource_disk_size_in_gib")]
        public double? ResourceDiskSizeInGiB { get; set; }

        /// <summary>
        /// The max number of data disks for this VM
        /// </summary>
        [TesTaskLogMetadataKey("vm_max_data_disk_count")]
        public int? MaxDataDiskCount { get; set; }

        /// <summary>
        /// HyperV Generations of VM this sku supports.
        /// </summary>
        [TesTaskLogMetadataKey("vm_hyper_v_generations")]
        public IEnumerable<string> HyperVGenerations { get; set; }

        /// <summary>
        /// List of regions this VM can be provisioned by Batch.
        /// </summary>
        [TesTaskLogMetadataKey("vm_regions_available")]
        public IEnumerable<string> RegionsAvailable { get; set; }
    }
}
