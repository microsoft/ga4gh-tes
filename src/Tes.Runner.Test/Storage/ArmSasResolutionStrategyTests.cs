// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;
using Moq;
using Tes.Runner.Storage;

namespace Tes.Runner.Test.Storage
{
    [TestClass, TestCategory("Unit")]
    public class ArmSasResolutionStrategyTests
    {
        private Mock<BlobServiceClient> mockBlobServiceClient = null!;
        private ArmSasResolutionStrategy armSasResolutionStrategy = null!;
        private UserDelegationKey userDelegationKey = null!;
        const string StorageAccountName = "foo";

        [TestInitialize]
        public void SetUp()
        {
            mockBlobServiceClient = new Mock<BlobServiceClient>();
            armSasResolutionStrategy = new ArmSasResolutionStrategy(_ => mockBlobServiceClient.Object);
            userDelegationKey = BlobsModelFactory.UserDelegationKey(Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), DateTimeOffset.UtcNow,
                DateTimeOffset.UtcNow.AddHours(1), "SIGNED_SERVICE", "V1_0", RunnerTestUtils.GenerateRandomTestAzureStorageKey());
            mockBlobServiceClient.Setup(c => c.GetUserDelegationKeyAsync(It.IsAny<DateTimeOffset?>(), It.IsAny<DateTimeOffset>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(Azure.Response.FromValue(userDelegationKey, null!));

        }

        [TestMethod]
        [DataRow($"https://{StorageAccountName}.blob.core.windows.net")]
        [DataRow($"https://{StorageAccountName}.blob.core.windows.net/cont")]
        [DataRow($"https://{StorageAccountName}.blob.core.windows.net/cont/blob")]
        public async Task CreateSasTokenWithStrategyAsync_ValidBlobStorageUrl_SasTokenIsGenerated(string sourceUrl)
        {
            var sasTokenUrl = await armSasResolutionStrategy.CreateSasTokenWithStrategyAsync(sourceUrl, BlobSasPermissions.Read);

            Assert.IsNotNull(sasTokenUrl);
            var blobUri = new Uri(sourceUrl);
            Assert.AreEqual(blobUri.Host, sasTokenUrl.Host);
            Assert.IsTrue(!string.IsNullOrEmpty(sasTokenUrl.Query));
        }

        [TestMethod]
        [DataRow($"https://storage.core.windows.net")]
        [DataRow($"https://foo.bar/cont")]
        [DataRow($"s3://foo.s3.bar")]
        public async Task CreateSasTokenWithStrategyAsync_InvalidBlobStorageUrl_UrlIsReturnAsIs(string sourceUrl)
        {
            var transformedUrl = await armSasResolutionStrategy.CreateSasTokenWithStrategyAsync(sourceUrl, BlobSasPermissions.Read);

            Assert.IsNotNull(transformedUrl);
            var blobUri = new Uri(sourceUrl);
            Assert.AreEqual(blobUri.AbsoluteUri, transformedUrl.AbsoluteUri);
        }

        [TestMethod]
        public async Task CreateSasTokenWithStrategyAsyncTest_CallTwiceForSameStorageAccount_CachesKey()
        {
            var sourceUrl = $"https://{StorageAccountName}.blob.core.windows.net";

            var sasTokenUrl1 = await armSasResolutionStrategy.CreateSasTokenWithStrategyAsync(sourceUrl, BlobSasPermissions.Read);
            var sasTokenUrl2 = await armSasResolutionStrategy.CreateSasTokenWithStrategyAsync(sourceUrl, BlobSasPermissions.Read);


            Assert.IsNotNull(sasTokenUrl1);
            Assert.IsNotNull(sasTokenUrl2);
            Assert.AreEqual(sasTokenUrl1, sasTokenUrl2);
            mockBlobServiceClient.Verify(c => c.GetUserDelegationKeyAsync(It.IsAny<DateTimeOffset?>(), It.IsAny<DateTimeOffset>(), It.IsAny<CancellationToken>()), Times.Once);
        }
    }
}
