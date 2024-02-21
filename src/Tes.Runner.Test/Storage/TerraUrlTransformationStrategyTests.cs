// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Sas;
using Moq;
using Tes.ApiClients;
using Tes.ApiClients.Models.Terra;
using Tes.Runner.Models;
using Tes.Runner.Storage;
using TesApi.Web.Management.Models.Terra;

namespace Tes.Runner.Test.Storage
{
    [TestClass, TestCategory("Unit")]
    public class TerraUrlTransformationStrategyTests
    {
        private const string TerraWsmApiHostUrl = "https://terra-wsm-api-host-url";

        private static readonly Guid workspaceId = Guid.NewGuid();
        private static readonly Guid containerResourceId = Guid.NewGuid();
        private static readonly string stubTerraBlobUrl = $"https://lz123abc123abc.blob.windows.net/sc-{workspaceId}";
        private const string StubBlobName = "stubBlobName";

        private const string StubSasToken =
            "sv=2022-08-22&ss=b&srt=sco&sp=rwdlacupx&se=2023-08-23T03:00:00Z&st=2022-08-22T19:31:04Z&spr=https&sig=XXXXXX";
        private TerraUrlTransformationStrategy transformationStrategy = null!;
        private TerraUrlTransformationStrategy secondTransformationStrategy = null!;
        private Mock<TerraWsmApiClient> mockTerraWsmApiClient = null!;
        private RuntimeOptions runtimeOptions = null!;
        private SasTokenApiParameters capturedSasTokenApiParameters = null!;
        private const int SasExpirationInSeconds = 5;

        [TestInitialize]
        public void SetUp()
        {
            runtimeOptions = new RuntimeOptions() { Terra = new TerraRuntimeOptions() { WsmApiHost = TerraWsmApiHostUrl } };
            mockTerraWsmApiClient = new Mock<TerraWsmApiClient>();
            capturedSasTokenApiParameters = new SasTokenApiParameters("", 0, "", "");
            SetupWsmClientWithAssumingSuccess();
            transformationStrategy = new TerraUrlTransformationStrategy(runtimeOptions.Terra, mockTerraWsmApiClient.Object, SasExpirationInSeconds);
            secondTransformationStrategy = new TerraUrlTransformationStrategy(runtimeOptions.Terra, mockTerraWsmApiClient.Object, SasExpirationInSeconds);
            transformationStrategy.ClearCache(); // Clear cache to avoid test interference, since cache is static in the class level scope.

        }

        private void SetupWsmClientWithAssumingSuccess()
        {
            mockTerraWsmApiClient
                .Setup(w => w.GetSasTokenAsync(It.IsAny<Guid>(),
                    It.IsAny<Guid>(), It.IsAny<SasTokenApiParameters>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(() => new WsmSasTokenApiResponse()
                {
                    Token = StubSasToken,
                    Url = $"{stubTerraBlobUrl}/{StubBlobName}?{StubSasToken}"
                })
                .Callback((Guid _1, Guid _2, SasTokenApiParameters sasTokenParams,
                    CancellationToken _3) =>
                {
                    capturedSasTokenApiParameters = sasTokenParams;
                });
            mockTerraWsmApiClient
                .Setup(w => w.GetContainerResourcesAsync(It.IsAny<Guid>(), It.IsAny<int>(), It.IsAny<int>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(() => new WsmListContainerResourcesResponse()
                {
                    Resources = new List<Resource>()
                    {
                        new Resource()
                        {
                            Metadata = new Metadata()
                            {
                                ResourceId = containerResourceId.ToString(),
                                WorkspaceId = workspaceId.ToString()
                            },
                            ResourceAttributes = new ResourceAttributes()
                            {
                                AzureStorageContainer = new AzureStorageContainer()
                                {
                                    // the storage container follows the naming convention of sc-{workspaceId}
                                    StorageContainerName = $"sc-{workspaceId}",
                                }
                            }
                        }
                    }
                });
        }

        [TestMethod]
        [DataRow(BlobSasPermissions.Add, "w")]
        [DataRow(BlobSasPermissions.Add | BlobSasPermissions.Create, "w")]
        [DataRow(BlobSasPermissions.Read, "r")]
        [DataRow(BlobSasPermissions.Delete, "d")]
        [DataRow(BlobSasPermissions.Add | BlobSasPermissions.Create | BlobSasPermissions.Read, "rw")]
        [DataRow(BlobSasPermissions.Add | BlobSasPermissions.Create | BlobSasPermissions.Read | BlobSasPermissions.Delete, "rwd")]
        public async Task TransformUrlWithStrategyAsync_BlobPermissionsProvided_AreConvertedToWsmPermissions(BlobSasPermissions blobSasPermissions, string wsmPermissions)
        {
            var sourceUrl = $"{stubTerraBlobUrl}/{StubBlobName}";

            await transformationStrategy.TransformUrlWithStrategyAsync(sourceUrl, blobSasPermissions);

            Assert.AreEqual(wsmPermissions, capturedSasTokenApiParameters.SasPermission);
        }

        [TestMethod]
        [DataRow("https://foo.blob.core.windows.net/container")]
        [DataRow("https://foo.bar.net/container")]
        [DataRow("https://foo.bar.net")]
        public async Task TransformUrlWithStrategyAsync_NonTerraStorageAccount_SourceUrlIsReturned(string sourceUrl)
        {
            var sasUrl = await transformationStrategy.TransformUrlWithStrategyAsync(sourceUrl, BlobSasPermissions.Read);

            Assert.AreEqual(new Uri(sourceUrl).ToString(), sasUrl.ToString());
        }

        [TestMethod]
        public async Task TransformUrlWithStrategyAsync_RequestsSasTokenMoreThanOnce_SasTokenIsCached()
        {
            var sourceUrl = $"{stubTerraBlobUrl}/blob";
            await transformationStrategy.TransformUrlWithStrategyAsync(sourceUrl, BlobSasPermissions.Read);
            await transformationStrategy.TransformUrlWithStrategyAsync(sourceUrl, BlobSasPermissions.Read);
            mockTerraWsmApiClient.Verify(w => w.GetSasTokenAsync(It.IsAny<Guid>(),
                               It.IsAny<Guid>(), It.IsAny<SasTokenApiParameters>(), It.IsAny<CancellationToken>()), Times.Once);
        }

        [TestMethod]
        public async Task TransformUrlWithStrategyAsync_RequestsSasTokenUsingMultipleStrategyInstances_SasTokenIsCachedAcrossInstances()
        {
            var sourceUrl = $"{stubTerraBlobUrl}/blob";
            await transformationStrategy.TransformUrlWithStrategyAsync(sourceUrl, BlobSasPermissions.Read);
            await secondTransformationStrategy.TransformUrlWithStrategyAsync(sourceUrl, BlobSasPermissions.Read);
            mockTerraWsmApiClient.Verify(w => w.GetSasTokenAsync(It.IsAny<Guid>(),
                It.IsAny<Guid>(), It.IsAny<SasTokenApiParameters>(), It.IsAny<CancellationToken>()), Times.Once);
        }

        [TestMethod]
        public async Task TransformUrlWithStrategyAsync_RequestsSasTokenMoreThanOnce_WsmResourceIdIsCached()
        {
            var sourceUrl = $"{stubTerraBlobUrl}/blob";
            await transformationStrategy.TransformUrlWithStrategyAsync(sourceUrl, BlobSasPermissions.Read);
            await transformationStrategy.TransformUrlWithStrategyAsync(sourceUrl, BlobSasPermissions.Read);
            await transformationStrategy.TransformUrlWithStrategyAsync(sourceUrl, BlobSasPermissions.Read);
            mockTerraWsmApiClient.Verify(w => w.GetContainerResourcesAsync(It.IsAny<Guid>(), It.IsAny<int>(), It.IsAny<int>(), It.IsAny<CancellationToken>()), Times.Once);
        }

        [TestMethod]
        public async Task TransformUrlWithStrategyAsync_RequestsSasTokenMoreThanOnceAfterExpiration_SasTokenIsRenewed()
        {
            var sourceUrl = $"{stubTerraBlobUrl}/blob";
            var sasUrl1 = await transformationStrategy.TransformUrlWithStrategyAsync(sourceUrl, BlobSasPermissions.Read);
            Assert.IsNotNull(sasUrl1);

            await Task.Delay(TimeSpan.FromSeconds(SasExpirationInSeconds + 1));

            var sasUrl2 = await transformationStrategy.TransformUrlWithStrategyAsync(sourceUrl, BlobSasPermissions.Read);
            Assert.IsNotNull(sasUrl2);
            mockTerraWsmApiClient.Verify(w => w.GetSasTokenAsync(It.IsAny<Guid>(),
                It.IsAny<Guid>(), It.IsAny<SasTokenApiParameters>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
        }
    }
}
