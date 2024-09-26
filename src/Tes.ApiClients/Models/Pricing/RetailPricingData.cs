// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json.Serialization;

namespace Tes.ApiClients.Models.Pricing
{
    /// <summary>
    /// A page of retail pricing data from the retail pricing API. 
    /// </summary>
    public class RetailPricingData
    {
        /// <summary>
        /// Billing currency.
        /// </summary>
        public string BillingCurrency { get; set; } = null!;

        /// <summary>
        /// Customer entity id. 
        /// </summary>
        public string CustomerEntityId { get; set; } = null!;

        /// <summary>
        /// Customer entity type.
        /// </summary>
        public string CustomerEntityType { get; set; } = null!;

        /// <summary>
        /// List of items in the page. 
        /// </summary>
        public PricingItem[] Items { get; set; } = null!;

        /// <summary>
        /// Next page link. 
        /// </summary>
        public string NextPageLink { get; set; } = null!;

        /// <summary>
        /// Count of items. 
        /// </summary>
        public int Count { get; set; }

        /// <summary>
        /// Request link
        /// </summary>
        public string RequestLink { get; set; } = null!;
    }

    [JsonSerializable(typeof(RetailPricingData))]
    public partial class RetailPricingDataContext : JsonSerializerContext
    { }
}
