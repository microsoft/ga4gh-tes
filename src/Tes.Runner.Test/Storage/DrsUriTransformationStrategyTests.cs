// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Moq;
using Tes.ApiClients;
using Tes.ApiClients.Models.Terra;
using Tes.Runner.Storage;

namespace Tes.Runner.Test.Storage
{
    [TestClass]
    [TestCategory("Unit")]
    public class DrsUriTransformationStrategyTests
    {
        private DrsUriTransformationStrategy drsUriTransformationStrategy = null!;
        private Mock<DrsHubApiClient> drsHubApiClientMock = null!;

        [TestInitialize]
        public void SetUp()
        {
            drsHubApiClientMock = new Mock<DrsHubApiClient>();
            drsUriTransformationStrategy = new DrsUriTransformationStrategy(drsHubApiClientMock.Object);
        }

        [TestMethod]
        public async Task TransformUriWithStrategyAsync_NonDrsUri_RerturnUriWithoutTransformation()
        {
            var uri = "https://non-drs-uri";

            var transformedUri = await drsUriTransformationStrategy.TransformUrlWithStrategyAsync(uri);

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

            var transformedUri = await drsUriTransformationStrategy.TransformUrlWithStrategyAsync(drsUri);

            Assert.AreEqual(new Uri(resolvedUrl), transformedUri);
        }

    }
}
