// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
//using Tes.Runner.Authentication;
using Tes.Runner.Models;

namespace Tes.Runner.Storage
{
    public class UrlTransformationStrategyFactory
    {
        public const string PassThroughUrl = "pass-through-url";
        public const string CloudProvider = "cloud-provider";
        public const string ArmUrl = "arm-url";
        public const string TerraUrl = "terra-url";
        public const string DrsUrl = "drs-url";


        //private readonly CredentialsManager TokenCredentialsManager = new();
        private readonly RuntimeOptions runtimeOptions;
        private readonly string apiVersion;
        private readonly Lazy<IUrlTransformationStrategy> passThroughFactory;
        private readonly Lazy<IUrlTransformationStrategy> cloudProviderFactory;
        private readonly Lazy<IUrlTransformationStrategy> armUrlFactory;
        private readonly Lazy<IUrlTransformationStrategy> terraUrlFactory;
        private readonly Lazy<IUrlTransformationStrategy> drsUrlFactory;

        // 
        public UrlTransformationStrategyFactory(
            RuntimeOptions runtimeOptions,
            [Microsoft.Extensions.DependencyInjection.FromKeyedServices(Executor.ApiVersion)] string apiVersion,
            [Microsoft.Extensions.DependencyInjection.FromKeyedServices(PassThroughUrl)] Lazy<IUrlTransformationStrategy> passThroughFactory,
            [Microsoft.Extensions.DependencyInjection.FromKeyedServices(CloudProvider)] Lazy<IUrlTransformationStrategy> cloudProviderFactory,
            [Microsoft.Extensions.DependencyInjection.FromKeyedServices(ArmUrl)] Lazy<IUrlTransformationStrategy> armUrlFactory,
            [Microsoft.Extensions.DependencyInjection.FromKeyedServices(TerraUrl)] Lazy<IUrlTransformationStrategy> terraUrlFactory,
            [Microsoft.Extensions.DependencyInjection.FromKeyedServices(DrsUrl)] Lazy<IUrlTransformationStrategy> drsUrlFactory)
        {
            ArgumentNullException.ThrowIfNull(runtimeOptions);
            ArgumentException.ThrowIfNullOrWhiteSpace(apiVersion);
            ArgumentNullException.ThrowIfNull(passThroughFactory);
            ArgumentNullException.ThrowIfNull(cloudProviderFactory);
            ArgumentNullException.ThrowIfNull(armUrlFactory);
            ArgumentNullException.ThrowIfNull(terraUrlFactory);
            ArgumentNullException.ThrowIfNull(drsUrlFactory);

            this.runtimeOptions = runtimeOptions;
            this.apiVersion = apiVersion;
            this.passThroughFactory = passThroughFactory;
            this.cloudProviderFactory = cloudProviderFactory;
            this.armUrlFactory = armUrlFactory;
            this.terraUrlFactory = terraUrlFactory;
            this.drsUrlFactory = drsUrlFactory;
        }

        /// <summary>
        /// Parameter-less constructor for mocking
        /// </summary>
        protected UrlTransformationStrategyFactory()
        {
            this.runtimeOptions = null!;
            this.apiVersion = null!;
            this.passThroughFactory = null!;
            this.cloudProviderFactory = null!;
            this.armUrlFactory = null!;
            this.terraUrlFactory = null!;
            this.drsUrlFactory = null!;
        }

        public IUrlTransformationStrategy CreateStrategy(TransformationStrategy transformationStrategy)
        {
            return transformationStrategy switch
            {
                TransformationStrategy.None => passThroughFactory.Value,
                TransformationStrategy.SchemeConverter => cloudProviderFactory.Value,
                TransformationStrategy.AzureResourceManager => armUrlFactory.Value,
                TransformationStrategy.TerraWsm => terraUrlFactory.Value,
                TransformationStrategy.CombinedTerra => CreateCombineTerraTransformationStrategy(runtimeOptions),
                TransformationStrategy.CombinedAzureResourceManager => CreateCombinedArmTransformationStrategy(runtimeOptions),
                _ => throw new NotImplementedException(),
            };
        }

        //public static BlobClientOptions GetClientOptions(Uri u, string apiVersion)
        //{
        //    var storageApiVersion = Enum.Parse<BlobClientOptions.ServiceVersion>($"V{apiVersion.Replace('-', '_')}");
        //    return new(storageApiVersion) { Audience = string.IsNullOrEmpty(u.AbsolutePath) ? new(ServiceUriAsAudience(u, asScope: false)) : default };

        //    static string? ServiceUriAsAudience(Uri uri, bool asScope) => string.IsNullOrEmpty(uri.AbsolutePath) ? uri.AbsoluteUri.TrimEnd('/') + (asScope ? "/.default" : string.Empty) : default;
        //}

        public static Func<Uri, BlobServiceClient> GetBlobServiceClientFactory(TokenCredential tokenCredential, string apiVersion)
        {
            var storageApiVersion = Enum.Parse<BlobClientOptions.ServiceVersion>($"V{apiVersion.Replace('-', '_')}");
            return u => new BlobServiceClient(
                u,
                tokenCredential,
                new(storageApiVersion) { Audience = string.IsNullOrEmpty(u.AbsolutePath) ? new(ServiceUriAsAudience(u, asScope: false)) : default });

            static string? ServiceUriAsAudience(Uri uri, bool asScope) => string.IsNullOrEmpty(uri.AbsolutePath) ? uri.AbsoluteUri.TrimEnd('/') + (asScope ? "/.default" : string.Empty) : default;
        }

        public CombinedTransformationStrategy CreateCombinedArmTransformationStrategy(RuntimeOptions runtimeOptions)
        {
            var strategy = new CombinedTransformationStrategy(
            [
                cloudProviderFactory.Value,
                armUrlFactory.Value,
            ]);

            InsertDrsStrategyIfTerraSettingsAreSet(strategy, runtimeOptions);

            return strategy;
        }

        private void InsertDrsStrategyIfTerraSettingsAreSet(CombinedTransformationStrategy strategy, RuntimeOptions runtime)
        {
            if (runtime.Terra is not null && !string.IsNullOrWhiteSpace(runtime.Terra.DrsHubApiHost))
            {
                strategy.InsertStrategy(0, drsUrlFactory.Value);
            }
        }

        public CombinedTransformationStrategy CreateCombineTerraTransformationStrategy(RuntimeOptions runtimeOptions)
        {
            var strategy = new CombinedTransformationStrategy(
            [
                cloudProviderFactory.Value,
                terraUrlFactory.Value,
            ]);

            InsertDrsStrategyIfTerraSettingsAreSet(strategy, runtimeOptions);

            return strategy;
        }

        public async Task<Uri> GetTransformedUrlAsync(RuntimeOptions runtimeOptions, StorageTargetLocation location, BlobSasPermissions permissions)
        {
            ArgumentNullException.ThrowIfNull(location);
            ArgumentNullException.ThrowIfNull(runtimeOptions);
            ArgumentException.ThrowIfNullOrEmpty(location.TargetUrl, nameof(location.TargetUrl));

            var strategy = CreateStrategy(location.TransformationStrategy);

            return await strategy.TransformUrlWithStrategyAsync(location.TargetUrl, permissions);
        }
    }
}
