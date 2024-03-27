// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using CommonUtilities;
using Moq;
using Tes.ApiClients;
using Tes.ApiClients.Models.Terra;
using Tes.Runner.Models;
using Tes.Runner.Storage;

namespace Tes.Runner.Tests.Storage
{
    [TestClass]
    [TestCategory("Unit")]
    public class DrsTransformationStrategyTests
    {
        private DrsTransformationStrategy drsTransformationStrategy = null!;
        private Mock<TokenCredential> tokenCredentialsMock = null!;
        private Mock<DrsHubApiClient> drsHubApiClientMock = null!;
        const string DrsHubApiHost = "https://drs-hub-api-host";

        [TestInitialize]
        public void SetUp()
        {
            tokenCredentialsMock = new Mock<TokenCredential>();
            var azureEnvConfig = new AzureEnvironmentConfig()
            {
                TokenScope = "https://management.azure.com/.default",
            };
            var terraRuntimeOptions = new TerraRuntimeOptions()
            {
                DrsHubApiHost = DrsHubApiHost,
            };
            drsHubApiClientMock = new Mock<DrsHubApiClient>();
            drsTransformationStrategy = new DrsTransformationStrategy(drsHubApiClientMock.Object);
        }

        [TestMethod]
        public async Task TransformUriWithStrategyAsync_NonDrsUri_RerturnUriWithoutTransformation()
        {
            var uri = "https://non-drs-uri";

            var transformedUri = await drsTransformationStrategy.TransformUrlWithStrategyAsync(uri);

            Assert.AreEqual(new Uri(uri), transformedUri);
        }

        [TestMethod]
        public async Task TransformUriWithStrategyAsync_DrsUri_CallsResolveApiAndReturnsResolvedUrl()
        {
            var drsUri = "drs://drs-uri";
            var resolvedUrl = "https://resolved-url";
            var response = new DrsResolveApiResponse()
            {
                AccessUrl = new AccessUrl()
                {
                    Url = resolvedUrl
                },
            };
            drsHubApiClientMock.Setup(x => x.ResolveDrsUriAsync(new Uri(drsUri), CancellationToken.None)).ReturnsAsync(response);

            var transformedUri = await drsTransformationStrategy.TransformUrlWithStrategyAsync(drsUri);

            Assert.AreEqual(new Uri(resolvedUrl), transformedUri);
        }

    }
}
