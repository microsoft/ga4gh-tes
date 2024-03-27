// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using Tes.Runner.Authentication;
using Tes.Runner.Models;

namespace Tes.Runner.Storage
{
    public static class UrlTransformationStrategyFactory
    {
        private static readonly CredentialsManager TokenCredentialsManager = new CredentialsManager();

        public static IUrlTransformationStrategy CreateStrategy(TransformationStrategy transformationStrategy, RuntimeOptions runtimeOptions)
        {
            switch (transformationStrategy)
            {
                case TransformationStrategy.None:
                    return new PassThroughUrlTransformationStrategy();
                case TransformationStrategy.SchemeConverter:
                    return new CloudProviderSchemeConverter();
                case TransformationStrategy.AzureResourceManager:
                    return new ArmUrlTransformationStrategy(u => new BlobServiceClient(u, TokenCredentialsManager.GetTokenCredential(runtimeOptions)), runtimeOptions);
                case TransformationStrategy.TerraWsm:
                    return new TerraUrlTransformationStrategy(runtimeOptions.Terra!, TokenCredentialsManager.GetTokenCredential(runtimeOptions), runtimeOptions.AzureEnvironmentConfig!);
                case TransformationStrategy.CombinedTerra:
                    return new CombinedTransformationStrategy(new List<IUrlTransformationStrategy>
                    {
                        new DrsUriTransformationStrategy(runtimeOptions.Terra!, TokenCredentialsManager.GetTokenCredential(runtimeOptions), runtimeOptions.AzureEnvironmentConfig!),
                        new CloudProviderSchemeConverter(),
                        new TerraUrlTransformationStrategy(runtimeOptions.Terra!, TokenCredentialsManager.GetTokenCredential(runtimeOptions), runtimeOptions.AzureEnvironmentConfig!),
                    });
                case TransformationStrategy.CombinedAzureResourceManager:
                    return new CombinedTransformationStrategy(new List<IUrlTransformationStrategy>
                    {
                        new DrsUriTransformationStrategy(runtimeOptions.Terra!, TokenCredentialsManager.GetTokenCredential(runtimeOptions), runtimeOptions.AzureEnvironmentConfig!),
                        new CloudProviderSchemeConverter(),
                        new ArmUrlTransformationStrategy(u => new BlobServiceClient(u, TokenCredentialsManager.GetTokenCredential(runtimeOptions)), runtimeOptions)
                    });
            }

            throw new NotImplementedException();
        }

        public static async Task<Uri> GetTransformedUrlAsync(RuntimeOptions runtimeOptions, StorageTargetLocation location, BlobSasPermissions permissions)
        {
            ArgumentNullException.ThrowIfNull(location);
            ArgumentNullException.ThrowIfNull(runtimeOptions);
            ArgumentException.ThrowIfNullOrEmpty(location.TargetUrl, nameof(location.TargetUrl));

            var strategy = CreateStrategy(location.TransformationStrategy, runtimeOptions);

            return await strategy.TransformUrlWithStrategyAsync(location.TargetUrl, permissions);
        }
    }
}
