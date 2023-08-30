// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Sas;

namespace Tes.Runner.Storage
{
    /// <summary>
    /// Transformation strategy that combines multiple strategies.
    /// Strategies are applied in the order they are provided.
    /// </summary>
    public class CombinedTransformationStrategy: IUrlTransformationStrategy
    {
        private readonly List<IUrlTransformationStrategy> strategies;

        public CombinedTransformationStrategy(List<IUrlTransformationStrategy> strategies)
        {
            ArgumentNullException.ThrowIfNull(strategies);

            this.strategies = strategies;
        }

        public async Task<Uri> TransformUrlWithStrategyAsync(string sourceUrl, BlobSasPermissions blobSasPermissions)
        {
            ArgumentException.ThrowIfNullOrEmpty(sourceUrl, nameof(sourceUrl));

            var result = new Uri(sourceUrl);

            foreach (var urlTransformationStrategy in strategies)
            {
                result = await urlTransformationStrategy.TransformUrlWithStrategyAsync(result.AbsoluteUri, blobSasPermissions);
            }

            return result;
        }
    }
}
