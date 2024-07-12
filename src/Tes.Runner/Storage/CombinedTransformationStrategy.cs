// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.ObjectModel;
using Azure.Storage.Sas;

namespace Tes.Runner.Storage
{
    /// <summary>
    /// Transformation strategy that combines multiple strategies.
    /// Strategies are applied in the order they are provided.
    /// </summary>
    public class CombinedTransformationStrategy : IUrlTransformationStrategy
    {
        private readonly List<IUrlTransformationStrategy> strategies;

        public CombinedTransformationStrategy(List<IUrlTransformationStrategy> strategies)
        {
            ArgumentNullException.ThrowIfNull(strategies);

            this.strategies = strategies;
        }

        public void InsertStrategy(int index, IUrlTransformationStrategy strategy)
        {
            ArgumentNullException.ThrowIfNull(strategy);
            strategies.Insert(index, strategy);
        }

        public async Task<Uri> TransformUrlWithStrategyAsync(string sourceUrl, BlobSasPermissions blobSasPermissions)
        {
            ArgumentException.ThrowIfNullOrEmpty(sourceUrl, nameof(sourceUrl));

            // All the UriFormatException-catching in this function is a workaround for DRS compact identifiers, which are
            // not IETL-compliant URIs because they contain colons in the "hostname" as a delimiter unrelated to ports.
            // Example: drs://anv0:v1_abc-123
            // These DRS URIs will be handled correctly by DrsUriTransformationStrategy, everything else will fail to
            // parse them and be ignored.
            Uri? result = null;
            try
            {
                result = new Uri(sourceUrl);
            }
            catch (UriFormatException)
            {
                // Do nothing, this is likely a DRS compact identifier
            }

            foreach (var urlTransformationStrategy in strategies)
            {
                try
                {
                    result = await urlTransformationStrategy.TransformUrlWithStrategyAsync(result != null ? result.AbsoluteUri : sourceUrl, blobSasPermissions);
                }
                catch (UriFormatException)
                {
                    // Do nothing, this is likely a DRS compact identifier
                }
            }

            // If we got through all the transformations without success, the URL is truly invalid
            // and we can now throw the relevant exception.
            return result ?? new Uri(sourceUrl);
        }

        public ReadOnlyCollection<IUrlTransformationStrategy> GetStrategies()
        {
            return strategies.AsReadOnly();
        }
    }
}
