// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Management.Models.Pricing
{
    /// <summary>
    /// A page of retail pricing data from the retail princing API. 
    /// </summary>
    public class RetailPricingData
    {
        /// <summary>
        /// Billing currency.
        /// </summary>
        public string BillingCurrency { get; set; }
        /// <summary>
        /// Customer entity id. 
        /// </summary>
        public string CustomerEntityId { get; set; }
        /// <summary>
        /// Customer entity type.
        /// </summary>
        public string CustomerEntityType { get; set; }
        /// <summary>
        /// List of items in the page. 
        /// </summary>
        public PricingItem[] Items { get; set; }
        /// <summary>
        /// Next page link. 
        /// </summary>
        public string NextPageLink { get; set; }
        /// <summary>
        /// Count of items. 
        /// </summary>
        public int Count { get; set; }
    }
}
