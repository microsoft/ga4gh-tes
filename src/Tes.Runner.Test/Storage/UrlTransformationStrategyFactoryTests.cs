// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using CommonUtilities;
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
        const string? DrsHubHost = "https://drsHub.bio";

        [TestInitialize]
        public void SetUp()
        {
            runtimeOptions = CreateRuntimeOptions();

            tokenCredentialMock = new Mock<TokenCredential>();
        }

        [TestMethod]
        public void CreateCombinedArmTransformationStrategy_WithDrsHubSettings_ReturnsExpectedStrategies()
        {
            runtimeOptions.Terra = CreateTerraRuntimeOptions(DrsHubHost);

            var combinedStrategy = UrlTransformationStrategyFactory.CreateCombinedArmTransformationStrategy(runtimeOptions, _ => tokenCredentialMock.Object, Runner.Transfer.BlobPipelineOptions.DefaultApiVersion);

            var internalStrategy = combinedStrategy.GetStrategies();

            Assert.IsNotNull(internalStrategy);
            Assert.IsInstanceOfType<DrsUriTransformationStrategy>(internalStrategy.ElementAt(0));
            Assert.IsInstanceOfType<CloudProviderSchemeConverter>(internalStrategy.ElementAt(1));
            Assert.IsInstanceOfType<ArmUrlTransformationStrategy>(internalStrategy.ElementAt(2));
        }

        [TestMethod]
        public void CreateCombinedArmTransformationStrategy_WithOutDrsHubSettings_ReturnsExpectedStrategies()
        {
            var combinedStrategy = UrlTransformationStrategyFactory.CreateCombinedArmTransformationStrategy(runtimeOptions, _ => tokenCredentialMock.Object, Runner.Transfer.BlobPipelineOptions.DefaultApiVersion);

            var internalStrategy = combinedStrategy.GetStrategies();

            Assert.IsNotNull(internalStrategy);
            Assert.IsInstanceOfType<CloudProviderSchemeConverter>(internalStrategy.ElementAt(0));
            Assert.IsInstanceOfType<ArmUrlTransformationStrategy>(internalStrategy.ElementAt(1));
        }

        [TestMethod]
        public void CreateCombinedTerraTransformationStrategy_WithOutDrsHubSettings_ReturnsExpectedStrategies()
        {
            runtimeOptions.Terra = CreateTerraRuntimeOptions();

            var combinedStrategy = UrlTransformationStrategyFactory.CreateCombineTerraTransformationStrategy(runtimeOptions, _ => tokenCredentialMock.Object);

            var internalStrategy = combinedStrategy.GetStrategies();

            Assert.IsNotNull(internalStrategy);
            Assert.IsInstanceOfType<CloudProviderSchemeConverter>(internalStrategy.ElementAt(0));
            Assert.IsInstanceOfType<TerraUrlTransformationStrategy>(internalStrategy.ElementAt(1));
        }

        [TestMethod]
        public void CreateCombinedTerraTransformationStrategy_WithDrsHubSettings_ReturnsExpectedStrategies()
        {
            runtimeOptions.Terra = CreateTerraRuntimeOptions(DrsHubHost);

            var combinedStrategy = UrlTransformationStrategyFactory.CreateCombineTerraTransformationStrategy(runtimeOptions, _ => tokenCredentialMock.Object);

            var internalStrategy = combinedStrategy.GetStrategies();

            Assert.IsNotNull(internalStrategy);
            Assert.IsInstanceOfType<DrsUriTransformationStrategy>(internalStrategy.ElementAt(0));
            Assert.IsInstanceOfType<CloudProviderSchemeConverter>(internalStrategy.ElementAt(1));
            Assert.IsInstanceOfType<TerraUrlTransformationStrategy>(internalStrategy.ElementAt(2));
        }

        private static RuntimeOptions CreateRuntimeOptions()
        {
            return new RuntimeOptions
            {
                AzureEnvironmentConfig = new AzureEnvironmentConfig()
                {
                    StorageUrlSuffix = @"core.windows.net",
                    TokenScope = ".default"
                }
            };
        }
        private static TerraRuntimeOptions CreateTerraRuntimeOptions(string? drsHubHost = default)
        {
            return new TerraRuntimeOptions
            {
                DrsHubApiHost = drsHubHost,
                WsmApiHost = "https://wsmhost.bio",
                LandingZoneApiHost = "https://lzhost.bio",
            };
        }
    }
}
