// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
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
        private const string WorkspaceStorageContainerName = TerraApiStubData.WorkspaceContainerName;

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
        [DataRow($"https://{WorkspaceStorageAccountName}.blob.core.windows.net/foo", true)]
        [DataRow($"https://bar.blob.core.windows.net/{WorkspaceStorageContainerName}", true)]
        public async Task IsHttpPublicAsync_StringScenario(string input, bool expectedResult)
        {
            var result = await terraStorageAccessProvider.IsPublicHttpUrlAsync(input);

            Assert.AreEqual(expectedResult, result);
        }

        [TestMethod]
        [DataRow($"{WorkspaceStorageAccountName}/{WorkspaceStorageContainerName}")]
        [DataRow($"/{WorkspaceStorageAccountName}/{WorkspaceStorageContainerName}")]
        [DataRow($"/{WorkspaceStorageAccountName}/{WorkspaceStorageContainerName}/")]
        [DataRow($"/{WorkspaceStorageAccountName}/{WorkspaceStorageContainerName}/dir/blobName")]
        [DataRow($"https://{WorkspaceStorageAccountName}.blob.core.windows.net/{WorkspaceStorageContainerName}")]
        [DataRow($"https://{WorkspaceStorageAccountName}.blob.core.windows.net/{WorkspaceStorageContainerName}/dir/blob")]
        public async Task MapLocalPathToSasUrlAsync_ValidInput(string input)
        {
            wsmApiClientMock.Setup(a => a.GetSasTokenAsync(terraApiStubData.WorkspaceId,
                    terraApiStubData.ContainerResourceId, It.IsAny<SasTokenApiParameters>()))
                .ReturnsAsync(terraApiStubData.GetWsmSasTokenApiResponse());

            var result = await terraStorageAccessProvider.MapLocalPathToSasUrlAsync(input);

            Assert.IsNotNull(terraApiStubData.GetWsmSasTokenApiResponse().Url, result);
        }

        [TestMethod]
        [DataRow($"{WorkspaceStorageAccountName}/foo")]
        [DataRow($"/bar/{WorkspaceStorageContainerName}")]
        [DataRow($"/foo/bar/")]
        [DataRow($"/foo/bar/dir/blobName")]
        [DataRow($"https://{WorkspaceStorageAccountName}.blob.core.windows.net/foo")]
        [DataRow($"https://bar.blob.core.windows.net/{WorkspaceStorageContainerName}/")]
        [ExpectedException(typeof(Exception))]
        public async Task MapLocalPathToSasUrlAsync_InvalidInputs(string input)
        {
            await terraStorageAccessProvider.MapLocalPathToSasUrlAsync(input);
        }

        [TestMethod]
        public async Task MapLocalPathToSasUrlAsync_ContainerSasTokenIsRequested()
        {
            var path = "/foo/bar";
            wsmApiClientMock.Setup(a => a.GetSasTokenAsync(terraApiStubData.WorkspaceId,
                    terraApiStubData.ContainerResourceId, It.IsAny<SasTokenApiParameters>()))
                .ReturnsAsync(terraApiStubData.GetWsmSasTokenApiResponse());

            var result = await terraStorageAccessProvider.MapLocalPathToSasUrlAsync(path, true);

            var builder = new UriBuilder(terraApiStubData.GetWsmSasTokenApiResponse().Url);
            builder.Path += path.TrimStart('/');

            Assert.IsNotNull(builder.ToString(), result);
        }
    }
}
