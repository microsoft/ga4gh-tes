﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading.Tasks;

namespace TesApi.Tests
{
    [TestClass]
    [TestCategory("unit")]
    public class TerraStorageAccessProviderTests
    {
        private const string WorkspaceStorageAccountName = "terraAccount";
        private const string WorkspaceStorageContainerName = "terraContainer";

        private Mock<CacheAndRetryHandler> cacheAndRetryHandlerMock;
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
            cacheAndRetryHandlerMock = new Mock<CacheAndRetryHandler>();
            wsmApiClientMock = new Mock<TerraWsmApiClient>();
            optionsMock = new Mock<IOptions<TerraOptions>>();
            terraOptions = CreateTerraOptionsFromStubData();
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

        [ExpectedException(typeof(Exception))]
        public async Task MapLocalPathToSasUrlAsync_InvalidInputs(string input)
        {
            var result = await terraStorageAccessProvider.MapLocalPathToSasUrlAsync(input);
        }

        private TerraOptions CreateTerraOptionsFromStubData()
        {
            return new TerraOptions()
            {
                WsmApiHost = terraApiStubData.WsmApiHost,
                WorkspaceStorageAccountName = WorkspaceStorageAccountName,
                WorkspaceStorageContainerName = WorkspaceStorageContainerName,
                WorkspaceId = terraApiStubData.WorkspaceId.ToString(),
                WorkspaceStorageContainerResourceId = terraApiStubData.ContainerResourceId.ToString(),
                SasTokenExpirationInSeconds = 60,
                SasAllowedIpRange = "169.0.0.1"
            };
        }
    }
}
