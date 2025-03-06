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
        private static readonly CredentialsManager TokenCredentialsManager = new();

        public static IUrlTransformationStrategy CreateStrategy(TransformationStrategy transformationStrategy, RuntimeOptions runtimeOptions, string apiVersion)
        {
            Func<string?, TokenCredential> credentialFactory = new(tokenScope => TokenCredentialsManager.GetTokenCredential(runtimeOptions, tokenScope));

            return transformationStrategy switch
            {
                TransformationStrategy.None => new PassThroughUrlTransformationStrategy(),
                TransformationStrategy.SchemeConverter => new CloudProviderSchemeConverter(),
                TransformationStrategy.AzureResourceManager => new ArmUrlTransformationStrategy(GetBlobServiceClientFactory(credentialFactory, apiVersion), runtimeOptions),
                TransformationStrategy.TerraWsm => new TerraUrlTransformationStrategy(runtimeOptions.Terra!, credentialFactory(null), runtimeOptions.AzureEnvironmentConfig!),
                TransformationStrategy.CombinedTerra => CreateCombineTerraTransformationStrategy(runtimeOptions, credentialFactory),
                TransformationStrategy.CombinedAzureResourceManager => CreateCombinedArmTransformationStrategy(runtimeOptions, credentialFactory, apiVersion),
                _ => throw new NotImplementedException(),
            };
        }

        private static Func<Uri, BlobServiceClient> GetBlobServiceClientFactory(Func<string?, TokenCredential> tokenCredentialFactory, string apiVersion)
        {
            var storageApiVersion = Enum.Parse<BlobClientOptions.ServiceVersion>($"V{apiVersion.Replace('-', '_')}");
            return u => new BlobServiceClient(
                u,
                tokenCredentialFactory(ServiceUriAsAudience(u, asScope: true)),
                new(storageApiVersion) { Audience = string.IsNullOrEmpty(u.AbsolutePath) ? new(ServiceUriAsAudience(u, asScope: false)) : default });

            static string? ServiceUriAsAudience(Uri uri, bool asScope) => string.IsNullOrEmpty(uri.AbsolutePath) ? uri.AbsoluteUri.TrimEnd('/') + (asScope ? "/.default" : string.Empty) : default;
        }

        public static CombinedTransformationStrategy CreateCombinedArmTransformationStrategy(RuntimeOptions runtimeOptions, Func<string?, TokenCredential> tokenCredentialFactory, string apiVersion)
        {
            var strategy = new CombinedTransformationStrategy(
            [
                new CloudProviderSchemeConverter(),
                new ArmUrlTransformationStrategy(GetBlobServiceClientFactory(tokenCredentialFactory, apiVersion), runtimeOptions)
            ]);

            InsertDrsStrategyIfTerraSettingsAreSet(strategy, runtimeOptions, tokenCredentialFactory(null));

            return strategy;
        }

        private static void InsertDrsStrategyIfTerraSettingsAreSet(CombinedTransformationStrategy strategy, RuntimeOptions runtime, TokenCredential tokenCredential)
        {
            if (runtime.Terra is not null && !string.IsNullOrWhiteSpace(runtime.Terra.DrsHubApiHost))
            {
                strategy.InsertStrategy(0, new DrsUriTransformationStrategy(runtime.Terra, tokenCredential, runtime.AzureEnvironmentConfig!));
            }
        }

        public static CombinedTransformationStrategy CreateCombineTerraTransformationStrategy(RuntimeOptions runtimeOptions, Func<string?, TokenCredential> tokenCredentialFactory)
        {
            var strategy = new CombinedTransformationStrategy(
            [
                new CloudProviderSchemeConverter(),
                new TerraUrlTransformationStrategy(runtimeOptions.Terra!, tokenCredentialFactory(null), runtimeOptions.AzureEnvironmentConfig!),
            ]);

            InsertDrsStrategyIfTerraSettingsAreSet(strategy, runtimeOptions, tokenCredentialFactory(null));

            return strategy;
        }

        public static async Task<Uri> GetTransformedUrlAsync(RuntimeOptions runtimeOptions, StorageTargetLocation location, BlobSasPermissions permissions, string apiVersion)
        {
            ArgumentNullException.ThrowIfNull(location);
            ArgumentNullException.ThrowIfNull(runtimeOptions);
            ArgumentException.ThrowIfNullOrEmpty(location.TargetUrl, nameof(location.TargetUrl));

            var strategy = CreateStrategy(location.TransformationStrategy, runtimeOptions, apiVersion);

            return await strategy.TransformUrlWithStrategyAsync(Environment.ExpandEnvironmentVariables(location.TargetUrl), permissions);
        }
    }
}
