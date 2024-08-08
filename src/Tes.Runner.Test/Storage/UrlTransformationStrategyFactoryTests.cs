// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using CommonUtilities;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Tes.Runner.Models;
using Tes.Runner.Storage;

namespace Tes.Runner.Test.Storage
{
    [TestClass]
    [TestCategory("Unit")]
    public class UrlTransformationStrategyFactoryTests
    {
        private RuntimeOptions runtimeOptions = null!;
        private Mock<TokenCredential> tokenCredentialMock = null!;
        private UrlTransformationStrategyFactory urlTransformationStrategyFactory = null!;
        const string? DrsHubHost = "https://drsHub.bio";

        [TestInitialize]
        public void SetUp()
        {
            runtimeOptions = CreateRuntimeOptions();

            tokenCredentialMock = new Mock<TokenCredential>();

            urlTransformationStrategyFactory = new(runtimeOptions, Runner.Transfer.BlobPipelineOptions.DefaultApiVersion,
                new(() => new PassThroughUrlTransformationStrategy()), new(() => new CloudProviderSchemeConverter()),
                new(() => new ArmUrlTransformationStrategy((_, _) => tokenCredentialMock.Object, runtimeOptions, Runner.Transfer.BlobPipelineOptions.DefaultApiVersion, NullLogger<ArmUrlTransformationStrategy>.Instance)),
                new(() => new TerraUrlTransformationStrategy(runtimeOptions, _ => tokenCredentialMock.Object, runtimeOptions.AzureEnvironmentConfig!, NullLogger<TerraUrlTransformationStrategy>.Instance)),
                new(() => new DrsUriTransformationStrategy(runtimeOptions, _ => tokenCredentialMock.Object, runtimeOptions.AzureEnvironmentConfig!, NullLogger<DrsUriTransformationStrategy>.Instance)));
        }

        [TestMethod]
        public void CreateCombinedArmTransformationStrategy_WithDrsHubSettings_ReturnsExpectedStrategies()
        {
            runtimeOptions.Terra = CreateTerraRuntimeOptions(DrsHubHost);

            var combinedStrategy = urlTransformationStrategyFactory.CreateCombinedArmTransformationStrategy(runtimeOptions);

            var internalStrategy = combinedStrategy.GetStrategies();

            Assert.IsNotNull(internalStrategy);
            Assert.IsInstanceOfType<DrsUriTransformationStrategy>(internalStrategy.ElementAt(0));
            Assert.IsInstanceOfType<CloudProviderSchemeConverter>(internalStrategy.ElementAt(1));
            Assert.IsInstanceOfType<ArmUrlTransformationStrategy>(internalStrategy.ElementAt(2));
        }

        [TestMethod]
        public void CreateCombinedArmTransformationStrategy_WithOutDrsHubSettings_ReturnsExpectedStrategies()
        {
            var combinedStrategy = urlTransformationStrategyFactory.CreateCombinedArmTransformationStrategy(runtimeOptions);

            var internalStrategy = combinedStrategy.GetStrategies();

            Assert.IsNotNull(internalStrategy);
            Assert.IsInstanceOfType<CloudProviderSchemeConverter>(internalStrategy.ElementAt(0));
            Assert.IsInstanceOfType<ArmUrlTransformationStrategy>(internalStrategy.ElementAt(1));
        }

        [TestMethod]
        public void CreateCombinedTerraTransformationStrategy_WithOutDrsHubSettings_ReturnsExpectedStrategies()
        {
            runtimeOptions.Terra = CreateTerraRuntimeOptions();

            var combinedStrategy = urlTransformationStrategyFactory.CreateCombineTerraTransformationStrategy(runtimeOptions);

            var internalStrategy = combinedStrategy.GetStrategies();

            Assert.IsNotNull(internalStrategy);
            Assert.IsInstanceOfType<CloudProviderSchemeConverter>(internalStrategy.ElementAt(0));
            Assert.IsInstanceOfType<TerraUrlTransformationStrategy>(internalStrategy.ElementAt(1));
        }

        [TestMethod]
        public void CreateCombinedTerraTransformationStrategy_WithDrsHubSettings_ReturnsExpectedStrategies()
        {
            runtimeOptions.Terra = CreateTerraRuntimeOptions(DrsHubHost);

            var combinedStrategy = urlTransformationStrategyFactory.CreateCombineTerraTransformationStrategy(runtimeOptions);

            var internalStrategy = combinedStrategy.GetStrategies();

            Assert.IsNotNull(internalStrategy);
            Assert.IsInstanceOfType<DrsUriTransformationStrategy>(internalStrategy.ElementAt(0));
            Assert.IsInstanceOfType<CloudProviderSchemeConverter>(internalStrategy.ElementAt(1));
            Assert.IsInstanceOfType<TerraUrlTransformationStrategy>(internalStrategy.ElementAt(2));
        }

        private static RuntimeOptions CreateRuntimeOptions()
        {
            return new()
            {
                AzureEnvironmentConfig = new AzureEnvironmentConfig(default, ".default", @"core.windows.net")
            };
        }
        private static TerraRuntimeOptions CreateTerraRuntimeOptions(string? drsHubHost = default)
        {
            return new()
            {
                DrsHubApiHost = drsHubHost,
                WsmApiHost = "https://wsmhost.bio",
                LandingZoneApiHost = "https://lzhost.bio",
            };
        }
    }
}
