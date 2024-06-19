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
        private TimeSpan cacheTTL = TimeSpan.FromMinutes(1);

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
            cacheAndRetryHandler.Value.Setup(c => c.ExecuteWithRetryConversionAndCachingAsync(
                    It.IsAny<string>(),
                    It.IsAny<Func<CancellationToken, Task<HttpResponseMessage>>>(),
                    It.IsAny<Func<HttpResponseMessage, CancellationToken, Task<SamActionManagedIdentityApiResponse>>>(),
                    It.IsAny<DateTimeOffset>(),
                    It.IsAny<CancellationToken>(),
                    It.IsAny<string>()))
                .ReturnsAsync(System.Text.Json.JsonSerializer.Deserialize<SamActionManagedIdentityApiResponse>(terraApiStubData.GetSamActionManagedIdentityApiResponseInJson())!);

            var apiResponse = await terraSamApiClient.GetActionManagedIdentityForACRPullAsync(terraApiStubData.AcrPullIdentitySamResourceId, cacheTTL, CancellationToken.None);

            Assert.IsNotNull(apiResponse);
            Assert.IsTrue(!string.IsNullOrEmpty(apiResponse.ObjectId));
            Assert.IsTrue(apiResponse.ObjectId.Contains(TerraApiStubData.TerraPetName));
        }

        [TestMethod]
        public async Task GetActionManagedIdentityAsync_ValidRequest_Returns404()
        {
            cacheAndRetryHandler.Value.Setup(c => c.ExecuteWithRetryConversionAndCachingAsync(
                    It.IsAny<string>(),
                    It.IsAny<Func<CancellationToken, Task<HttpResponseMessage>>>(),
                    It.IsAny<Func<HttpResponseMessage, CancellationToken, Task<SamActionManagedIdentityApiResponse>>>(),
                    It.IsAny<DateTimeOffset>(),
                    It.IsAny<CancellationToken>(),
                    It.IsAny<string>()))
                .Throws(new HttpRequestException(null, null, System.Net.HttpStatusCode.NotFound));

            var apiResponse = await terraSamApiClient.GetActionManagedIdentityForACRPullAsync(terraApiStubData.AcrPullIdentitySamResourceId, cacheTTL, CancellationToken.None);

            Assert.IsNull(apiResponse);
        }

        [TestMethod]
        public async Task GetActionManagedIdentityAsync_ValidRequest_Returns500()
        {
            cacheAndRetryHandler.Value.Setup(c => c.ExecuteWithRetryConversionAndCachingAsync(
                    It.IsAny<string>(),
                    It.IsAny<Func<CancellationToken, Task<HttpResponseMessage>>>(),
                    It.IsAny<Func<HttpResponseMessage, CancellationToken, Task<SamActionManagedIdentityApiResponse>>>(),
                    It.IsAny<DateTimeOffset>(),
                    It.IsAny<CancellationToken>(),
                    It.IsAny<string>()))
                .Throws(new HttpRequestException(null, null, System.Net.HttpStatusCode.BadGateway));

            await Assert.ThrowsExceptionAsync<HttpRequestException>(async () => await terraSamApiClient.GetActionManagedIdentityForACRPullAsync(terraApiStubData.AcrPullIdentitySamResourceId, cacheTTL, CancellationToken.None));
        }
    }
}
