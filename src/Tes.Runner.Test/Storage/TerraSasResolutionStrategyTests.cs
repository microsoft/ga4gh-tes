// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Sas;
using Microsoft.Azure.Management.BatchAI.Fluent.Models;
using Microsoft.Azure.Management.Network.Fluent;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Tes.ApiClients;
using Tes.ApiClients.Models.Terra;
using Tes.Runner.Models;
using Tes.Runner.Storage;
using TesApi.Web.Management.Models.Terra;

namespace Tes.Runner.Test.Storage
{
    [TestClass, TestCategory("Unit")]
    public class TerraSasResolutionStrategyTests
    {
        private const string TerraWsmApiHostUrl = "https://terra-wsm-api-host-url";

        private static readonly Guid workspaceId = Guid.NewGuid();
        private static readonly Guid containerResourceId = Guid.NewGuid();
        private static readonly string stubTerraBlobUrl = $"https://lz123abc123abc.blob.windows.net/sc-{workspaceId}";
        private const string StubBlobName = "stubBlobName";

        private const string StubSasToken =
            "sv=2022-08-22&ss=b&srt=sco&sp=rwdlacupx&se=2023-08-23T03:00:00Z&st=2022-08-22T19:31:04Z&spr=https&sig=XXXXXX";
        private TerraSasResolutionStrategy resolutionStrategy;
        private Mock<TerraWsmApiClient> mockTerraWsmApiClient;
        private RuntimeOptions runtimeOptions;
        private SasTokenApiParameters capturedSasTokenApiParameters;
        private Guid captureWskId = Guid.Empty;

        [TestInitialize]
        public void SetUp()
        {
            runtimeOptions = new RuntimeOptions() { Terra = new TerraRuntimeOptions() { WsmApiHost = TerraWsmApiHostUrl } };
            mockTerraWsmApiClient = new Mock<TerraWsmApiClient>();
            capturedSasTokenApiParameters = new SasTokenApiParameters("", 0, "", "");
            captureWskId = Guid.Empty;
            SetupWsmClientWithAssumingSuccess();
            resolutionStrategy = new TerraSasResolutionStrategy(runtimeOptions.Terra, mockTerraWsmApiClient.Object);
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
                .Callback((Guid wksId, Guid resourceId, SasTokenApiParameters sasTokenParams,
                    CancellationToken cancellationToken) =>
                {
                    capturedSasTokenApiParameters = sasTokenParams;
                    captureWskId = wksId;
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
        public async Task CreateSasTokenWithStrategyAsync_BlobPermissionsProvided_AreConvertedToWsmPermissions(BlobSasPermissions blobSasPermissions, string wsmPermissions)
        {
            var sourceUrl = $"{stubTerraBlobUrl}/{StubBlobName}";

            var sasUrl = await resolutionStrategy.CreateSasTokenWithStrategyAsync(sourceUrl, blobSasPermissions);

            Assert.AreEqual(wsmPermissions, capturedSasTokenApiParameters.SasPermission);
        }

        [TestMethod]
        [DataRow("https://foo.blob.core.windows.net/container")]
        [DataRow("https://foo.bar.net/container")]
        [DataRow("https://foo.bar.net")]
        public async Task CreateSasTokenWithStrategyAsync_NonTerraStorageAccount_SourceUrlIsReturned(string sourceUrl)
        {
            var sasUrl = await resolutionStrategy.CreateSasTokenWithStrategyAsync(sourceUrl, BlobSasPermissions.Read);

            Assert.AreEqual(new Uri(sourceUrl).ToString(), sasUrl.ToString());
        }
    }
}
