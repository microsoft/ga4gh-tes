// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using Azure.Storage.Sas;
using CommonUtilities;
using Microsoft.Extensions.Logging;
using Tes.ApiClients;
using Tes.Runner.Models;
using Tes.Runner.Transfer;

namespace Tes.Runner.Storage
{
    /// <summary>
    /// Terra DRS transformation strategy. Uses the configured DRSHub service to resolve a DRS URI.
    /// If the URI is not a valid DRS URI, the URI is not transformed.
    /// </summary>
    public class DrsUriTransformationStrategy : IUrlTransformationStrategy
    {
        private readonly ILogger<DrsUriTransformationStrategy> logger = PipelineLoggerFactory.Create<DrsUriTransformationStrategy>();
        private readonly DrsHubApiClient drsHubApiClient;

        static DrsUriTransformationStrategy()
        {
            if (!UriParser.IsKnownScheme(DrsUriParser.UriSchemeDrs))
            {
                DrsUriParser.Register();
            }
        }

        public DrsUriTransformationStrategy(DrsHubApiClient drsHubApiClient)
        {
            ArgumentNullException.ThrowIfNull(drsHubApiClient);

            this.drsHubApiClient = drsHubApiClient;
        }
        public DrsUriTransformationStrategy(TerraRuntimeOptions terraRuntimeOptions, TokenCredential tokenCredential, AzureEnvironmentConfig azureCloudIdentityConfig)
        {
            ArgumentNullException.ThrowIfNull(terraRuntimeOptions);
            ArgumentException.ThrowIfNullOrEmpty(terraRuntimeOptions.DrsHubApiHost, nameof(terraRuntimeOptions.DrsHubApiHost));

            drsHubApiClient = DrsHubApiClient.CreateDrsHubApiClient(terraRuntimeOptions.DrsHubApiHost, tokenCredential, azureCloudIdentityConfig);
        }

        public async Task<Uri> TransformUrlWithStrategyAsync(string sourceUrl, BlobSasPermissions blobSasPermissions = 0)
        {
            Uri sourceUri = new(sourceUrl);

            if (!ContainsDrsScheme(sourceUri.Scheme))
            {
                return sourceUri;
            }

            var response = await drsHubApiClient.ResolveDrsUriAsync(sourceUri);

            try
            {
                var sasUri = new Uri(response.AccessUrl.Url);

                return sasUri;
            }
            catch (Exception e)
            {
                //we are not logging the response content here as it may contain sensitive information
                logger.LogError(e, "The URL returned by DRSHub API is invalid.");
                throw;
            }
        }

        private static bool ContainsDrsScheme(string scheme)
        {
            return scheme.Equals(DrsUriParser.UriSchemeDrs, StringComparison.OrdinalIgnoreCase);
        }
    }
}
