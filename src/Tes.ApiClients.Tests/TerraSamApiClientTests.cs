// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using CommonUtilities;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Tes.ApiClients.Models.Terra;

namespace Tes.ApiClients.Tests
{
    [TestClass, TestCategory("Unit")]
    public class TerraSamApiClientTests
    {
        private TerraSamApiClient terraSamApiClient = null!;
        private Mock<TokenCredential> tokenCredential = null!;
        private Mock<CachingRetryPolicyBuilder> cacheAndRetryBuilder = null!;
        private Lazy<Mock<CachingRetryHandler.CachingAsyncRetryHandlerPolicy<HttpResponseMessage>>> cacheAndRetryHandler = null!;
        private TerraApiStubData terraApiStubData = null!;
        private AzureEnvironmentConfig azureEnvironmentConfig = null!;

        [TestInitialize]
        public void SetUp()
        {
            terraApiStubData = new TerraApiStubData();
            tokenCredential = new Mock<TokenCredential>();
            cacheAndRetryBuilder = new Mock<CachingRetryPolicyBuilder>();
            var cache = new Mock<Microsoft.Extensions.Caching.Memory.IMemoryCache>();
            cache.Setup(c => c.CreateEntry(It.IsAny<object>())).Returns(new Mock<Microsoft.Extensions.Caching.Memory.ICacheEntry>().Object);
            cacheAndRetryBuilder.SetupGet(c => c.AppCache).Returns(cache.Object);
            cacheAndRetryHandler = new(TestServices.RetryHandlersHelpers.GetCachingAsyncRetryPolicyMock(cacheAndRetryBuilder, c => c.DefaultRetryHttpResponseMessagePolicyBuilder()));
            azureEnvironmentConfig = ExpensiveObjectTestUtility.AzureCloudConfig.AzureEnvironmentConfig!;

            terraSamApiClient = new TerraSamApiClient(TerraApiStubData.SamApiHost, tokenCredential.Object,
                cacheAndRetryBuilder.Object, azureEnvironmentConfig, NullLogger<TerraSamApiClient>.Instance);
        }

        [TestMethod]
        public async Task GetActionManagedIdentityAsync_ValidRequest_ReturnsPayload()
        {
            cacheAndRetryHandler.Value.Setup(c => c.ExecuteWithRetryAndConversionAsync(It.IsAny<Func<CancellationToken, Task<HttpResponseMessage>>>(), It.IsAny<Func<HttpResponseMessage, CancellationToken, Task<SamActionManagedIdentityApiResponse>>>(), It.IsAny<CancellationToken>(), It.IsAny<string>()))
                .ReturnsAsync(System.Text.Json.JsonSerializer.Deserialize<SamActionManagedIdentityApiResponse>(terraApiStubData.GetSamActionManagedIdentityApiResponseInJson())!);

            var apiResponse = await terraSamApiClient.GetActionManagedIdentityForACRPullAsync(terraApiStubData.AcrPullIdentitySamResourceId, CancellationToken.None);

            Assert.IsNotNull(apiResponse);
            Assert.IsTrue(!string.IsNullOrEmpty(apiResponse.ObjectId));
            Assert.IsTrue(apiResponse.ObjectId.Contains(TerraApiStubData.TerraPetName));
        }
    }
}
