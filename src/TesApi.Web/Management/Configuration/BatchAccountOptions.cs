// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Management.Configuration
{
    /// <summary>
    /// Batch account configuration options. 
    /// </summary>
    public class BatchAccountOptions
    {
        /// <summary>
        /// Configuration section.
        /// </summary>
        public const string SectionName = "BatchAccount";
        /// <summary>
        /// Default Azure offer durable id. 
        /// </summary>
        public const string DefaultAzureOfferDurableId = "MS-AZR-0003p";

        /// <summary>
        /// Account name.
        /// </summary>
        public string AccountName { get; set; }
        /// <summary>
        /// Base URl. 
        /// <remarks>Required if AppKey is provided.</remarks>
        /// </summary>
        public string BaseUrl { get; set; }
        /// <summary>
        /// AppKey
        /// <remarks>If not set ARM authentication is used. </remarks>
        /// </summary>
        public string AppKey { get; set; }
        /// <summary>
        /// Arm region where the batch account is located.
        /// <remarks>Required if AppKey is provided.</remarks>
        /// </summary>
        public string Region { get; set; }
        /// <summary>
        /// Subscription Id of the batch account. 
        /// <remarks>Required if AppKey is provided.</remarks>
        /// </summary>
        public string SubscriptionId { get; set; }
        /// <summary>
        /// ResourceApiResponse group of the batch account. 
        /// <remarks>Required if AppKey is provided.</remarks>
        /// </summary>
        public string ResourceGroup { get; set; }
        /// <summary>
        /// Azure offer id. 
        /// </summary>
        public string AzureOfferDurableId { get; set; } = DefaultAzureOfferDurableId;
    }
}
