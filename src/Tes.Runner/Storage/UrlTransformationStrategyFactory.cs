// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
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
                    return CreateCombineTerraTransformationStrategy(runtimeOptions, TokenCredentialsManager.GetTokenCredential(runtimeOptions));
                case TransformationStrategy.CombinedAzureResourceManager:
                    return CreateCombinedArmTransformationStrategy(runtimeOptions, TokenCredentialsManager.GetTokenCredential(runtimeOptions));
            }

            throw new NotImplementedException();
        }

        public static CombinedTransformationStrategy CreateCombinedArmTransformationStrategy(RuntimeOptions runtimeOptions, TokenCredential tokenCredential)
        {
            var strategy = new CombinedTransformationStrategy(new List<IUrlTransformationStrategy>
            {
                new CloudProviderSchemeConverter(),
                new ArmUrlTransformationStrategy(u => new BlobServiceClient(u, tokenCredential), runtimeOptions)
            });

            InsertDrsStrategyIfTerraSettingsAreSet(strategy, runtimeOptions, tokenCredential);

            return strategy;
        }

        private static void InsertDrsStrategyIfTerraSettingsAreSet(CombinedTransformationStrategy strategy, RuntimeOptions runtime, TokenCredential tokenCredential)
        {
            if (runtime.Terra != null && !String.IsNullOrWhiteSpace(runtime.Terra.DrsHubApiHost))
            {
                strategy.InsertStrategy(0, new DrsUriTransformationStrategy(runtime.Terra, tokenCredential, runtime.AzureEnvironmentConfig!));
            }
        }

        public static CombinedTransformationStrategy CreateCombineTerraTransformationStrategy(RuntimeOptions runtimeOptions, TokenCredential tokenCredential)
        {
            var strategy = new CombinedTransformationStrategy(new List<IUrlTransformationStrategy>
            {
                new CloudProviderSchemeConverter(),
                new TerraUrlTransformationStrategy(runtimeOptions.Terra!, tokenCredential, runtimeOptions.AzureEnvironmentConfig!),
            });

            InsertDrsStrategyIfTerraSettingsAreSet(strategy, runtimeOptions, tokenCredential);

            return strategy;
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
