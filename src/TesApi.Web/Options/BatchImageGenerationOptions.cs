// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Options
{
    /// <summary>
    /// Base class for BatchImageGenerationOptions
    /// </summary>
    public abstract class BatchImageGenerationOptionsBase
    {
        /// <summary>
        /// Azure Batch image offer
        /// </summary>
        public string BatchImageOffer { get; set; }
        /// <summary>
        /// Azure Batch image publisher
        /// </summary>
        public string BatchImagePublisher { get; set; }
        /// <summary>
        /// Azure Batch image SKU
        /// </summary>
        public string BatchImageSku { get; set; }
        /// <summary>
        /// Azure Batch image version
        /// </summary>
        public string BatchImageVersion { get; set; }
        ///// <summary>
        ///// Azure Batch node agent sku ID
        ///// </summary>
        //public string BatchNodeAgentSkuId { get; set; }
    }

    /// <summary>
    /// Generation 1 Batch Node information
    /// </summary>
    public class BatchImageGeneration1Options : BatchImageGenerationOptionsBase
    {
        /// <summary>
        /// BatchImage configuration section
        /// </summary>
        public const string SectionName = "BatchImageGen1";
    }

    /// <summary>
    /// Generation 2 Batch Node information
    /// </summary>
    public class BatchImageGeneration2Options : BatchImageGenerationOptionsBase
    {
        /// <summary>
        /// BatchImage configuration section
        /// </summary>
        public const string SectionName = "BatchImageGen2";
    }
}
