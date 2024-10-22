// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Models
{
    /// <summary>
    /// VmDataDisks contains the runtime specs for additional attached data disks
    /// </summary>
    public readonly record struct VmDataDisks()
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="VmDataDisks"/> struct.
        /// </summary>
        /// <param name="pricePerHour">Disk price per hour.</param>
        /// <param name="lun">Disk identity in VM.</param>
        /// <param name="capacity">Initial size of disk in GiB.</param>
        /// <param name="caching">Type of caching for the disk.</param>
        /// <param name="storageAccountType">Storage account type for creating disk.</param>
        public VmDataDisks(decimal pricePerHour, int lun, int capacity, string caching, string storageAccountType)
            : this()
        {
            PricePerHour = pricePerHour;
            Lun = lun;
            CapacityInGiB = capacity;
            Caching = caching;
            StorageAccountType = storageAccountType;
        }

        /// <summary>
        /// Disk price per hour
        /// </summary>
        //[TesTaskLogMetadataKey("vm_price_per_hour_usd")]
        public decimal PricePerHour { get; }

        /// <summary>
        /// The lun is used to uniquely identify each data disk. If attaching multiple disks, each should have a distinct lun. The value must be between 0 and 63, inclusive.
        /// </summary>
        //[TesTaskLogMetadataKey("vm_price_per_hour_usd")]
        public int Lun { get; }

        /// <summary>
        /// The initial disk size in GB when creating new data disk.
        /// </summary>
        //[TesTaskLogMetadataKey("vm_price_per_hour_usd")]
        public int CapacityInGiB { get; }

        /// <summary>
        /// The type of caching to enable for the disk.
        /// </summary>
        //[TesTaskLogMetadataKey("vm_price_per_hour_usd")]
        public string Caching { get; }

        /// <summary>
        /// The storage account type for use in creating data disks or OS disk.
        /// </summary>
        //[TesTaskLogMetadataKey("vm_price_per_hour_usd")]
        public string StorageAccountType { get; }
    }
}
