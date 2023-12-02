﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Net;
using System.Web;
using Azure.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Tes.ApiClients.Models.Terra;
using TesApi.Web.Management.Models.Terra;

namespace Tes.ApiClients.Tests
{
    [TestClass, TestCategory("Unit")]
    public class TerraWsmApiClientTests
    {
        private TerraWsmApiClient terraWsmApiClient = null!;
        private Mock<TokenCredential> tokenCredential = null!;
        private Mock<CachingRetryHandler> cacheAndRetryHandler = null!;
        private TerraApiStubData terraApiStubData = null!;

        [TestInitialize]
        public void SetUp()
        {
            terraApiStubData = new TerraApiStubData();
            tokenCredential = new Mock<TokenCredential>();
            cacheAndRetryHandler = new Mock<CachingRetryHandler>();
            terraWsmApiClient = new TerraWsmApiClient(TerraApiStubData.WsmApiHost, tokenCredential.Object,
                cacheAndRetryHandler.Object, NullLogger<TerraWsmApiClient>.Instance);
        }

        [TestMethod]
        public void GetContainerSasTokenApiUri_NoSasParameters_ReturnsExpectedUriWithoutQueryString()
        {
            var uri = terraWsmApiClient.GetSasTokenApiUrl(terraApiStubData.WorkspaceId,
                terraApiStubData.ContainerResourceId, null!);

            var expectedUri =
                $"{TerraApiStubData.WsmApiHost}/api/workspaces/v1/{terraApiStubData.WorkspaceId}/resources/controlled/azure/storageContainer/{terraApiStubData.ContainerResourceId}/getSasToken";
            Assert.AreEqual(expectedUri, uri.ToString());
            Assert.AreEqual(string.Empty, uri.Query);
        }

        [TestMethod]
        public void GetContainerSasTokenApiUri_WithAllSasParameters_ReturnsExpectedUriWithQueryString()
        {
            var sasParams = new SasTokenApiParameters("ipRange", 10, "rwdl", "blobName");

            var uri = terraWsmApiClient.GetSasTokenApiUrl(terraApiStubData.WorkspaceId,
                terraApiStubData.ContainerResourceId, sasParams);

            var expectedUri =
                $"{TerraApiStubData.WsmApiHost}/api/workspaces/v1/{terraApiStubData.WorkspaceId}/resources/controlled/azure/storageContainer/{terraApiStubData.ContainerResourceId}/getSasToken"
                + $"?sasIpRange={sasParams.SasIpRange}&sasExpirationDuration={sasParams.SasExpirationInSeconds}&sasPermissions={sasParams.SasPermission}&sasBlobName={sasParams.SasBlobName}";

            var parsedQs = HttpUtility.ParseQueryString(uri.Query);

            Assert.AreEqual(expectedUri, uri.ToString());
            Assert.AreEqual(parsedQs["sasIpRange"], sasParams.SasIpRange);
            Assert.AreEqual(parsedQs["sasExpirationDuration"], sasParams.SasExpirationInSeconds.ToString());
            Assert.AreEqual(parsedQs["sasPermissions"], sasParams.SasPermission);
            Assert.AreEqual(parsedQs["sasBlobName"], sasParams.SasBlobName);
        }

        [TestMethod]
        public void GetContainerSasTokenApiUri_WithSomeSasParameters_ReturnsExpectedUriWithQueryString()
        {
            var sasParams = new SasTokenApiParameters("ipRange", 10, null!, null!);

            var uri = terraWsmApiClient.GetSasTokenApiUrl(terraApiStubData.WorkspaceId,
                terraApiStubData.ContainerResourceId, sasParams);

            var expectedUri =
                $"{TerraApiStubData.WsmApiHost}/api/workspaces/v1/{terraApiStubData.WorkspaceId}/resources/controlled/azure/storageContainer/{terraApiStubData.ContainerResourceId}/getSasToken"
                + $"?sasIpRange={sasParams.SasIpRange}&sasExpirationDuration={sasParams.SasExpirationInSeconds}";

            var parsedQs = HttpUtility.ParseQueryString(uri.Query);

            Assert.AreEqual(expectedUri, uri.ToString());
            Assert.AreEqual(parsedQs["sasIpRange"], sasParams.SasIpRange);
            Assert.AreEqual(parsedQs["sasExpirationDuration"], sasParams.SasExpirationInSeconds.ToString());
        }

        [TestMethod]
        public async Task GetSasTokenAsync_ValidRequest_ReturnsPayload()
        {
            var response = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(terraApiStubData.GetWsmSasTokenApiResponseInJson())
            };

            cacheAndRetryHandler.Setup(c => c.ExecuteWithRetryAsync(It.IsAny<Func<CancellationToken, Task<HttpResponseMessage>>>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(response);

            var apiResponse = await terraWsmApiClient.GetSasTokenAsync(terraApiStubData.WorkspaceId,
                terraApiStubData.ContainerResourceId, null!, CancellationToken.None);

            Assert.IsNotNull(apiResponse);
            Assert.IsTrue(!string.IsNullOrEmpty(apiResponse.Token));
            Assert.IsTrue(!string.IsNullOrEmpty(apiResponse.Url));
        }

        [TestMethod]
        public async Task GetContainerResourcesAsync_ValidRequest_ReturnsPayload()
        {
            var response = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(terraApiStubData.GetContainerResourcesApiResponseInJson())
            };

            cacheAndRetryHandler.Setup(c => c.ExecuteWithRetryAsync(It.IsAny<Func<CancellationToken, Task<HttpResponseMessage>>>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(response);

            var apiResponse = await terraWsmApiClient.GetContainerResourcesAsync(terraApiStubData.WorkspaceId,
                offset: 0, limit: 10, CancellationToken.None);

            Assert.IsNotNull(apiResponse);
            Assert.AreEqual(1, apiResponse.Resources.Count);
            Assert.IsTrue(apiResponse.Resources.Any(r => r.Metadata.ResourceId.ToString().Equals(terraApiStubData.ContainerResourceId.ToString(), StringComparison.OrdinalIgnoreCase)));
        }

        [TestMethod]
        public async Task DeleteBatchPoolAsync_204Response_Succeeds()
        {
            var wsmResourceId = Guid.NewGuid();
            var response = new HttpResponseMessage(HttpStatusCode.NoContent);

            cacheAndRetryHandler.Setup(c => c.ExecuteWithRetryAsync(It.IsAny<Func<CancellationToken, Task<HttpResponseMessage>>>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(response);

            await terraWsmApiClient.DeleteBatchPoolAsync(terraApiStubData.WorkspaceId, wsmResourceId, CancellationToken.None);
        }

        [TestMethod]
        public void GetDeleteBatchPoolUrl_ValidWorkspaceAndResourceId_ValidWSMUrl()
        {
            var wsmResourceId = Guid.NewGuid();

            var url = terraWsmApiClient.GetDeleteBatchPoolUrl(terraApiStubData.WorkspaceId, wsmResourceId);

            Assert.IsNotNull(url);
            var expectedUrl = $"{TerraApiStubData.WsmApiHost}/api/workspaces/v1/{terraApiStubData.WorkspaceId}/resources/controlled/azure/batchpool/{wsmResourceId}";
            Assert.AreEqual(expectedUrl, url);
        }

        [TestMethod]
        public async Task GetResourceQuotaAsync_ValidResourceIdReturnsQuotaInformationAndGetsAuthToken()
        {
            var body = terraApiStubData.GetResourceQuotaApiResponseInJson();
            cacheAndRetryHandler.Setup(c => c.ExecuteWithRetryAndCachingAsync(It.IsAny<string>(),
                    It.IsAny<Func<CancellationToken, Task<string>>>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(body);

            var quota = await terraWsmApiClient.GetResourceQuotaAsync(terraApiStubData.WorkspaceId, terraApiStubData.BatchAccountId, cacheResults: true, cancellationToken: CancellationToken.None);

            Assert.IsNotNull(quota);
            Assert.AreEqual(terraApiStubData.LandingZoneId, quota.LandingZoneId);
            Assert.AreEqual(terraApiStubData.BatchAccountId, quota.AzureResourceId);
            Assert.AreEqual("Microsoft.Batch/batchAccounts", quota.ResourceType);
            Assert.AreEqual(100, quota.QuotaValues.PoolQuota);
            Assert.IsTrue(quota.QuotaValues.DedicatedCoreQuotaPerVmFamilyEnforced);
            Assert.AreEqual(300, quota.QuotaValues.ActiveJobAndJobScheduleQuota);
            Assert.AreEqual(350, quota.QuotaValues.DedicatedCoreQuota);
            Assert.AreEqual(59, quota.QuotaValues.DedicatedCoreQuotaPerVmFamily.Count);
            Assert.AreEqual("standardLSv2Family", quota.QuotaValues.DedicatedCoreQuotaPerVmFamily.Keys.First());
            Assert.AreEqual(0, quota.QuotaValues.DedicatedCoreQuotaPerVmFamily.Values.First());
            tokenCredential.Verify(t => t.GetTokenAsync(It.IsAny<TokenRequestContext>(),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [TestMethod]
        public async Task GetLandingZoneResourcesAsync_ListOfLandingZoneResourcesAndGetsAuthToken()
        {
            var body = terraApiStubData.GetResourceApiResponseInJson();

            cacheAndRetryHandler.Setup(c => c.ExecuteWithRetryAndCachingAsync(It.IsAny<string>(),
                    It.IsAny<Func<CancellationToken, Task<string>>>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(body);

            var resources = await terraWsmApiClient.GetLandingZoneResourcesAsync(terraApiStubData.WorkspaceId, CancellationToken.None);

            Assert.IsNotNull(resources);
            Assert.AreEqual(terraApiStubData.LandingZoneId, resources.Id);
            Assert.AreEqual(5, resources.Resources.Length);
            tokenCredential.Verify(t => t.GetTokenAsync(It.IsAny<TokenRequestContext>(),
                    It.IsAny<CancellationToken>()),
                Times.Once);

        }

        [TestMethod]
        public void GetLandingZoneResourcesApiUrl_CorrectUrlIsParsed()
        {
            var url = terraWsmApiClient.GetLandingZoneResourcesApiUrl(terraApiStubData.WorkspaceId);
            var expectedUrl = $"{TerraApiStubData.WsmApiHost}/api/workspaces/v1/{terraApiStubData.WorkspaceId}/resources/controlled/azure/landingzone";

            Assert.AreEqual(expectedUrl, url.ToString());

        }

        [TestMethod]
        public void GetQuotaApiUrl_CorrectUrlIsParsed()
        {
            var url = terraWsmApiClient.GetQuotaApiUrl(terraApiStubData.WorkspaceId, terraApiStubData.BatchAccountId);
            var expectedUrl = $"{TerraApiStubData.WsmApiHost}/api/workspaces/v1/{terraApiStubData.WorkspaceId}/resources/controlled/azure/landingzone/quota?azureResourceId={Uri.EscapeDataString(terraApiStubData.BatchAccountId)}";

            Assert.AreEqual(expectedUrl, url.ToString());

        }

    }
}
