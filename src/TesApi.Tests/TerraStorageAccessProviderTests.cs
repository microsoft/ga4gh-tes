// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using CommonUtilities;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Tes.ApiClients;
using Tes.ApiClients.Models.Terra;
using Tes.Models;
using TesApi.Web;
using TesApi.Web.Management.Configuration;
using TesApi.Web.Options;
using TesApi.Web.Storage;

namespace TesApi.Tests
{
    [TestClass]
    [TestCategory("unit")]
    public class TerraStorageAccessProviderTests
    {
        private const string WorkspaceStorageAccountName = TerraApiStubData.WorkspaceAccountName;
        private const string WorkspaceStorageContainerName = TerraApiStubData.WorkspaceStorageContainerName;
        private const string BatchSchedulingPrefix = "prefix-foo.bar";

        private Mock<TerraWsmApiClient> wsmApiClientMock;
        private Mock<IAzureProxy> azureProxyMock;
        private TerraStorageAccessProvider terraStorageAccessProvider;
        private TerraApiStubData terraApiStubData;
        private Mock<IOptions<TerraOptions>> optionsMock;
        private TerraOptions terraOptions;
        private BatchSchedulingOptions batchSchedulingOptions;
        private SasTokenApiParameters capturedTokenApiParameters = null!;

        [TestInitialize]
        public void SetUp()
        {
            capturedTokenApiParameters = new SasTokenApiParameters("", 0, "", "");
            terraApiStubData = new TerraApiStubData();
            wsmApiClientMock = new Mock<TerraWsmApiClient>();
            optionsMock = new Mock<IOptions<TerraOptions>>();
            terraOptions = terraApiStubData.GetTerraOptions();
            batchSchedulingOptions = new BatchSchedulingOptions() { Prefix = BatchSchedulingPrefix };
            optionsMock.Setup(o => o.Value).Returns(terraOptions);
            azureProxyMock = new Mock<IAzureProxy>();
            var config = ExpensiveObjectTestUtility.AzureCloudConfig.AzureEnvironmentConfig;
            terraStorageAccessProvider = new TerraStorageAccessProvider(wsmApiClientMock.Object, azureProxyMock.Object, optionsMock.Object, Options.Create(batchSchedulingOptions), config, NullLogger<TerraStorageAccessProvider>.Instance);
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

            Assert.IsNotNull(terraApiStubData.GetWsmSasTokenApiResponse().Url, result?.AbsoluteUri);
        }

        private void SetUpTerraApiClient()
        {
            wsmApiClientMock.Setup(a => a.GetSasTokenAsync(
                    terraApiStubData.GetWorkspaceIdFromContainerName(WorkspaceStorageContainerName),
                    terraApiStubData.ContainerResourceId, It.IsAny<SasTokenApiParameters>(), It.IsAny<CancellationToken>()))
                .Callback((Guid _, Guid _, SasTokenApiParameters apiParameters, CancellationToken _) =>
                    {
                        capturedTokenApiParameters = apiParameters;
                    })
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

            var result = await terraStorageAccessProvider.MapLocalPathToSasUrlAsync(input + blobPath, CancellationToken.None, getContainerSas: true);

            Assert.IsNotNull(result);
            Assert.AreEqual($"{expected}?sv={TerraApiStubData.SasToken}", result.AbsoluteUri);
        }

        [TestMethod]
        [DataRow($"{WorkspaceStorageAccountName}/foo")]
        [DataRow($"/bar/{WorkspaceStorageContainerName}")]
        [DataRow($"/foo/bar/")]
        [DataRow($"/foo/bar/dir/blobName")]
        [DataRow($"https://bar.blob.core.windows.net/{WorkspaceStorageContainerName}/")]
        [DataRow($"https://bar.blob.core.windows.net/container/")]
        [ExpectedException(typeof(InvalidOperationException))]
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
            var uri = await terraStorageAccessProvider.GetMappedSasUrlFromWsmAsync(blobInfo, CancellationToken.None);

            Assert.IsNotNull(uri);
            Assert.AreEqual(uri.AbsolutePath, $"/{TerraApiStubData.WorkspaceStorageContainerName}/blobName");
        }

        [TestMethod]
        [DataRow("script/foo.sh")]
        [DataRow("/script/foo.sh")]
        public async Task GetInternalTesBlobUrlAsync_BlobPathIsProvided_ReturnsValidURLWithWsmContainerAndTesPrefixAppended(
            string blobName)
        {
            SetUpTerraApiClient();

            var uri = await terraStorageAccessProvider.GetInternalTesBlobUrlAsync(blobName, CancellationToken.None);

            Assert.IsNotNull(uri);
            Assert.AreEqual($"/{TerraApiStubData.WorkspaceStorageContainerName}/{batchSchedulingOptions.Prefix}{StorageAccessProvider.TesExecutionsPathPrefix}/{blobName.TrimStart('/')}", uri.AbsolutePath);
        }

        [TestMethod]
        [DataRow("script/foo.sh", "/script/foo.sh")]
        [DataRow("/script/foo.sh", "/script/foo.sh")]
        [DataRow("", "")]
        public async Task GetInternalTesTaskBlobUrlAsync_BlobPathIsProvided_ReturnsValidURLWithWsmContainerTaskIdAndTesPrefixAppended(
            string blobName, string expectedBlobName)
        {
            SetUpTerraApiClient();
            var task = new TesTask { Name = "taskName", Id = Guid.NewGuid().ToString() };
            var uri = await terraStorageAccessProvider.GetInternalTesTaskBlobUrlAsync(task, blobName, CancellationToken.None);

            Assert.IsNotNull(uri);
            Assert.AreEqual($"/{TerraApiStubData.WorkspaceStorageContainerName}/{batchSchedulingOptions.Prefix}{StorageAccessProvider.TesExecutionsPathPrefix}/{task.Id}{expectedBlobName}", uri.AbsolutePath);
        }

        [TestMethod]
        [DataRow("script/foo.sh", "/script/foo.sh")]
        [DataRow("/script/foo.sh", "/script/foo.sh")]
        [DataRow("", "")]
        public async Task GetInternalTesTaskBlobUrlAsync_BlobPathAndInternalPathPrefixIsProvided_ReturnsValidURLWithWsmContainerTaskIdAndInternalPathPrefixAppended(
            string blobName, string expectedBlobName)
        {
            var internalPathPrefix = "internalPathPrefix";

            SetUpTerraApiClient();
            var task = new TesTask { Name = "taskName", Id = Guid.NewGuid().ToString() };
            task.Resources = new TesResources();
            task.Resources.BackendParameters = new Dictionary<string, string>
            {
                { TesResources.SupportedBackendParameters.internal_path_prefix.ToString(), internalPathPrefix }
            };

            var uri = await terraStorageAccessProvider.GetInternalTesTaskBlobUrlAsync(task, blobName, CancellationToken.None);

            Assert.IsNotNull(uri);
            Assert.AreEqual($"/{TerraApiStubData.WorkspaceStorageContainerName}/{internalPathPrefix}{expectedBlobName}", uri.AbsolutePath);
        }

        [TestMethod]
        [DataRow("/script/foo.sh", "/prefix")]
        [DataRow("script/foo.sh", "/prefix")]
        [DataRow("/script/foo.sh", "prefix")]
        [DataRow("script/foo.sh", "prefix")]
        public async Task GetInternalTesTaskBlobUrlAsync_BlobPathAndInternalPathPrefixAreProvided_BlobNameDoesNotStartWithSlashWhenCallingWSM(
            string blobPath, string internalPrefix)
        {

            SetUpTerraApiClient();
            var task = new TesTask { Name = "taskName", Id = Guid.NewGuid().ToString() };
            task.Resources = new TesResources();
            task.Resources.BackendParameters = new Dictionary<string, string>
            {
                { TesResources.SupportedBackendParameters.internal_path_prefix.ToString(), internalPrefix }
            };

            var uri = await terraStorageAccessProvider.GetInternalTesTaskBlobUrlAsync(task, blobPath, CancellationToken.None);

            Assert.IsNotNull(uri);
            Assert.AreNotEqual('/', capturedTokenApiParameters.SasBlobName[0]);
        }

        [TestMethod]
        [DataRow("", $"https://{WorkspaceStorageAccountName}.blob.core.windows.net/{WorkspaceStorageContainerName}/{BatchSchedulingPrefix}{StorageAccessProvider.TesExecutionsPathPrefix}")]
        [DataRow("blob", $"https://{WorkspaceStorageAccountName}.blob.core.windows.net/{WorkspaceStorageContainerName}/{BatchSchedulingPrefix}{StorageAccessProvider.TesExecutionsPathPrefix}/blob")]
        public void GetInternalTesBlobUrlWithoutSasToken_BlobPathIsProvided_ExpectedUrl(string blobPath,
            string expectedUrl)
        {
            var uri = terraStorageAccessProvider.GetInternalTesBlobUrlWithoutSasToken(blobPath);

            Assert.AreEqual(expectedUrl, uri.AbsoluteUri);
        }
    }
}
