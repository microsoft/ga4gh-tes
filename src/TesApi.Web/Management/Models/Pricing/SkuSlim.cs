// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Management.Models.Pricing
{
    /// <summary>
    /// Sku information from the retail pricing API.
    /// </summary>
    public class SkuSlim
    {
        /// <summary>
        /// Arm region.
        /// </summary>
        public string ArmRegionName { get; set; }
        /// <summary>
        /// Sku name.
        /// </summary>
        public string SkuName { get; set; }
        /// <summary>
        /// Low priority sku flag.
        /// </summary>
        public bool IsLowPriority { get; set; }
        /// <summary>
        /// Price per hour in USD.
        /// </summary>
        public double PricePerHour { get; set; }
    }
}
