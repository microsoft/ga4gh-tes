using System;

namespace TesApi.Web.Management.Models.Pricing
{
    /// <summary>
    /// A pricing item.
    /// </summary>
    public class PricingItem
    {
        /// <summary>
        /// Currency code. 
        /// </summary>
        public string currencyCode { get; set; }
        /// <summary>
        /// Tier minimum units.
        /// </summary>
        public float tierMinimumUnits { get; set; }
        /// <summary>
        /// Retail price.
        /// </summary>
        public float retailPrice { get; set; }
        /// <summary>
        /// Unit price.
        /// </summary>
        public float unitPrice { get; set; }
        /// <summary>
        /// ARM region name.
        /// </summary>
        public string armRegionName { get; set; }
        /// <summary>
        /// Location.
        /// </summary>
        public string location { get; set; }
        /// <summary>
        /// Effective start date.
        /// </summary>
        public DateTime effectiveStartDate { get; set; }
        /// <summary>
        /// Meter id. 
        /// </summary>
        public string meterId { get; set; }
        /// <summary>
        /// Meter name
        /// </summary>
        public string meterName { get; set; }
        /// <summary>
        /// Product id. 
        /// </summary>
        public string productId { get; set; }
        /// <summary>
        /// Sku id. 
        /// </summary>
        public string skuId { get; set; }
        /// <summary>
        /// Product name.
        /// </summary>
        public string productName { get; set; }
        /// <summary>
        /// Sku name. 
        /// </summary>
        public string skuName { get; set; }
        /// <summary>
        /// Service name.
        /// </summary>
        public string serviceName { get; set; }
        /// <summary>
        /// Service id. 
        /// </summary>
        public string serviceId { get; set; }
        /// <summary>
        /// Service family. 
        /// </summary>
        public string serviceFamily { get; set; }
        /// <summary>
        /// Unit of measure.
        /// </summary>
        public string unitOfMeasure { get; set; }
        /// <summary>
        /// Type.
        /// </summary>
        public string type { get; set; }
        /// <summary>
        /// if it is primary meter region.
        /// </summary>
        public bool isPrimaryMeterRegion { get; set; }
        /// <summary>
        /// Sku name. 
        /// </summary>
        public string armSkuName { get; set; }
        /// <summary>
        /// Reservation term.
        /// </summary>
        public string reservationTerm { get; set; }
    }
}
