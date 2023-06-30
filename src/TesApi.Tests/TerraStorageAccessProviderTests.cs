// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using TesApi.Web;
using TesApi.Web.Management.Clients;
using TesApi.Web.Management.Configuration;
using TesApi.Web.Management.Models.Terra;
using TesApi.Web.Storage;

namespace TesApi.Tests
{
    [TestClass]
    [TestCategory("unit")]
    public class TerraStorageAccessProviderTests
    {
        private const string WorkspaceStorageAccountName = TerraApiStubData.WorkspaceAccountName;
        private const string WorkspaceStorageContainerName = TerraApiStubData.WorkspaceStorageContainerName;

        private Mock<TerraWsmApiClient> wsmApiClientMock;
        private Mock<IAzureProxy> azureProxyMock;
        private TerraStorageAccessProvider terraStorageAccessProvider;
        private TerraApiStubData terraApiStubData;
        private Mock<IOptions<TerraOptions>> optionsMock;
        private TerraOptions terraOptions;

        [TestInitialize]
        public void SetUp()
        {
            terraApiStubData = new TerraApiStubData();
            wsmApiClientMock = new Mock<TerraWsmApiClient>();
            optionsMock = new Mock<IOptions<TerraOptions>>();
            terraOptions = terraApiStubData.GetTerraOptions();
            optionsMock.Setup(o => o.Value).Returns(terraOptions);
            azureProxyMock = new Mock<IAzureProxy>();
            terraStorageAccessProvider = new TerraStorageAccessProvider(NullLogger<TerraStorageAccessProvider>.Instance,
                optionsMock.Object, azureProxyMock.Object, wsmApiClientMock.Object);
        }

        [TestMethod]
        [DataRow("http://foo.bar", true)]
        [DataRow("https://foo.bar", true)]
        [DataRow("sb://foo.bar", false)]
        [DataRow("/foo/bar", false)]
        [DataRow("foo/bar", false)]
        [DataRow($"https://{WorkspaceStorageAccountName}.blob.core.windows.net/{WorkspaceStorageContainerName}", false)]
        [DataRow($"https://{WorkspaceStorageAccountName}.blob.core.windows.net/foo", false)]
        [DataRow($"https://bar.blob.core.windows.net/{WorkspaceStorageContainerName}", true)]
        public async Task IsHttpPublicAsync_StringScenario(string input, bool expectedResult)
        {
            var result = await terraStorageAccessProvider.IsPublicHttpUrlAsync(input, CancellationToken.None);

            Assert.AreEqual(expectedResult, result);
        }

        [TestMethod]
        [DataRow($"https://{WorkspaceStorageAccountName}.blob.core.windows.net/{WorkspaceStorageContainerName}")]
        [DataRow($"https://{WorkspaceStorageAccountName}.blob.core.windows.net/{WorkspaceStorageContainerName}/dir/blob")]
        public async Task MapLocalPathToSasUrlAsync_ValidInput(string input)
        {
            SetUpTerraApiClient();

            var result = await terraStorageAccessProvider.MapLocalPathToSasUrlAsync(input, CancellationToken.None);

            Assert.IsNotNull(terraApiStubData.GetWsmSasTokenApiResponse().Url, result);
        }

        private void SetUpTerraApiClient()
        {
            wsmApiClientMock.Setup(a => a.GetSasTokenAsync(
                    terraApiStubData.GetWorkspaceIdFromContainerName(WorkspaceStorageContainerName),
                    terraApiStubData.ContainerResourceId, It.IsAny<SasTokenApiParameters>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(terraApiStubData.GetWsmSasTokenApiResponse());
            wsmApiClientMock.Setup(a =>
                    a.GetContainerResourcesAsync(It.IsAny<Guid>(), It.IsAny<int>(), It.IsAny<int>(),
                        It.IsAny<CancellationToken>()))
                .ReturnsAsync(terraApiStubData.GetWsmContainerResourcesApiResponse());
        }

        [TestMethod]
        [DataRow($"https://{WorkspaceStorageAccountName}.blob.core.windows.net/{WorkspaceStorageContainerName}", "", TerraApiStubData.WsmGetSasResponseStorageUrl)]
        [DataRow($"https://{WorkspaceStorageAccountName}.blob.core.windows.net/{WorkspaceStorageContainerName}", "/dir/blob", $"{TerraApiStubData.WsmGetSasResponseStorageUrl}/dir/blob")]
        public async Task MapLocalPathToSasUrlAsync_GetContainerSasIsTrue(string input, string blobPath, string expected)
        {
            SetUpTerraApiClient();

            var result = await terraStorageAccessProvider.MapLocalPathToSasUrlAsync(input + blobPath, CancellationToken.None, true);

            Assert.IsNotNull(result);
            Assert.AreEqual($"{expected}?sv={TerraApiStubData.SasToken}", result);
        }

        [TestMethod]
        [DataRow($"{WorkspaceStorageAccountName}/foo")]
        [DataRow($"/bar/{WorkspaceStorageContainerName}")]
        [DataRow($"/foo/bar/")]
        [DataRow($"/foo/bar/dir/blobName")]
        [DataRow($"https://bar.blob.core.windows.net/{WorkspaceStorageContainerName}/")]
        [DataRow($"https://bar.blob.core.windows.net/container/")]
        [ExpectedException(typeof(InvalidOperationException))]
        //[ExpectedException(typeof(Exception))]
        public async Task MapLocalPathToSasUrlAsync_InvalidStorageAccountInputs(string input)
        {
            await terraStorageAccessProvider.MapLocalPathToSasUrlAsync(input, CancellationToken.None);
        }

        [TestMethod]
        [DataRow("")]
        [DataRow("blobName")]
        public async Task GetMappedSasUrlFromWsmAsync_WithOrWithOutBlobName_ReturnsValidURLWithBlobName(string responseBlobName)
        {
            SetUpTerraApiClient();

            var blobInfo = new TerraBlobInfo(terraApiStubData.GetWorkspaceIdFromContainerName(WorkspaceStorageContainerName), terraApiStubData.ContainerResourceId, TerraApiStubData.WorkspaceStorageContainerName, "blobName");
            var url = await terraStorageAccessProvider.GetMappedSasUrlFromWsmAsync(blobInfo, CancellationToken.None);

            Assert.IsNotNull(url);
            var uri = new Uri(url);

            Assert.AreEqual(uri.AbsolutePath, $"/{TerraApiStubData.WorkspaceStorageContainerName}/blobName");
        }
    }
}
