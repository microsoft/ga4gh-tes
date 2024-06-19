// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using Azure.Storage.Sas;
using CommonUtilities;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
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
        private readonly ILogger logger;
        private readonly DrsHubApiClient drsHubApiClient;
        private const string DrsScheme = "drs";

        internal DrsUriTransformationStrategy(DrsHubApiClient drsHubApiClient)
        {
            ArgumentNullException.ThrowIfNull(drsHubApiClient);

            this.drsHubApiClient = drsHubApiClient;
            this.logger = NullLogger.Instance;
        }

        public DrsUriTransformationStrategy(RuntimeOptions runtimeOptions, Func<RuntimeOptions, TokenCredential> tokenCredentialFactory, AzureEnvironmentConfig azureCloudIdentityConfig, ILogger<DrsUriTransformationStrategy> logger)
        {
            ArgumentNullException.ThrowIfNull(runtimeOptions);
            ArgumentNullException.ThrowIfNull(runtimeOptions.Terra, nameof(runtimeOptions.Terra));
            ArgumentNullException.ThrowIfNull(tokenCredentialFactory);
            ArgumentException.ThrowIfNullOrEmpty(runtimeOptions.Terra!.DrsHubApiHost, nameof(runtimeOptions.Terra.DrsHubApiHost));
            ArgumentNullException.ThrowIfNull(logger);

            drsHubApiClient = DrsHubApiClient.CreateDrsHubApiClient(runtimeOptions.Terra!.DrsHubApiHost, tokenCredentialFactory(runtimeOptions), azureCloudIdentityConfig);
            this.logger = logger;
        }

        public async Task<Uri> TransformUrlWithStrategyAsync(string sourceUrl, BlobSasPermissions blobSasPermissions = 0)
        {
            var sourceUri = new Uri(sourceUrl);

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
            return scheme.Equals(DrsScheme, StringComparison.OrdinalIgnoreCase);
        }
    }
}
