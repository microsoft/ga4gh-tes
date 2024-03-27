// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using CommonUtilities.Options;
using Microsoft.Azure.Management.Compute.Fluent.Models;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace Tes.ApiClients.Tests
{
    [TestClass]
    [TestCategory("Unit")]
    public class DrsHubApiClientTests
    {
        private Mock<TokenCredential> tokenCredentialsMock;
        private CachingRetryPolicyBuilder cachingRetryPolicyBuilder;
        private CommonUtilities.AzureEnvironmentConfig azureEnvironmentConfig;
        private DrsHubApiClient apiClient;

        private const string DrsApiHost = "https://drshub.foo";

        [TestInitialize]
        public void Setup()
        {
            var retryPolicyOptions = new RetryPolicyOptions();
            var appCache = new MemoryCache(new MemoryCacheOptions());
            cachingRetryPolicyBuilder = new CachingRetryPolicyBuilder(appCache, Options.Create(retryPolicyOptions));

            tokenCredentialsMock = new Mock<TokenCredential>();
            azureEnvironmentConfig = new CommonUtilities.AzureEnvironmentConfig()
            {
                TokenScope = "https://management.azure.com/.default"
            };
            apiClient = new DrsHubApiClient(DrsApiHost, tokenCredentialsMock.Object, cachingRetryPolicyBuilder, azureEnvironmentConfig, NullLogger<DrsHubApiClient>.Instance);
        }

        [TestMethod]
        public void CreateDrsHubApiClient_ReturnsValidDrsApiClient()
        {
            var drsApiClient = DrsHubApiClient.CreateDrsHubApiClient("https://drshub.foo", tokenCredentialsMock.Object, azureEnvironmentConfig);

            Assert.IsNotNull(drsApiClient);
        }

        [TestMethod]
        public async Task GetDrsResolveRequestContent_ValidDrsUri_ReturnsValidRequestContentWithExpectedValues()
        {
            var drsUriString = "drs://drs.foo";
            var drsUri = new Uri(drsUriString);
            var content = await apiClient.GetDrsResolveRequestContent(drsUri).ReadAsStringAsync();

            Assert.IsNotNull(ExpectedDrsResolveRequestJson, content);
        }

        [TestMethod]
        public async Task GetDrsResolveApiResponse_ResponseWithAccessUrl_CanDeserializeJSon() { 
            var httpResponse = new HttpResponseMessage(System.Net.HttpStatusCode.OK);
            httpResponse.Content = new StringContent(ExpectedRsResolveResponseJson);

            var drsResolveResponse = await DrsHubApiClient.GetDrsResolveApiResponseAsync(httpResponse, CancellationToken.None);

            Assert.IsNotNull(drsResolveResponse);
            Assert.IsNotNull(drsResolveResponse.AccessUrl);
            Assert.AreEqual("https://storage.foo/bar",drsResolveResponse.AccessUrl.Url);
        }

        private const string ExpectedRsResolveResponseJson = @"{
            ""accessUrl"": {
            ""url"": ""https://storage.foo/bar"",
            ""headers"": null
            }
        }";

        private const string ExpectedDrsResolveRequestJson = @"{
                        ""url"": ""drs://drs.foo"",
                        ""cloudPlatform"": ""azure"",
                        ""fields"":[""accessUrl""]
        }";
    }
}
