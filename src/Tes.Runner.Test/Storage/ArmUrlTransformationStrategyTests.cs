// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;
using CommonUtilities;
using Moq;
using Tes.Runner.Models;
using Tes.Runner.Storage;

namespace Tes.Runner.Test.Storage
{
    [TestClass, TestCategory("Unit")]
    public class ArmUrlTransformationStrategyTests
    {
        private Mock<BlobServiceClient> mockBlobServiceClient = null!;
        private Mock<Runner.Transfer.BlobApiHttpUtils> mockBlobApiHttpUtils = null!;
        private ArmUrlTransformationStrategy armUrlTransformationStrategy = null!;
        private UserDelegationKey userDelegationKey = null!;
        const string StorageAccountName = "foo";
        const string SasToken = "sv=2019-12-12&ss=bfqt&srt=sco&spr=https&st=2023-09-27T17%3A32%3A57Z&se=2023-09-28T17%3A32%3A57Z&sp=rwdlacupx&sig=SIGNATURE";

        [TestInitialize]
        public void SetUp()
        {
            mockBlobServiceClient = new Mock<BlobServiceClient>();
            RuntimeOptions options = new()
            {
                AzureEnvironmentConfig = AzureEnvironmentConfig.FromArmEnvironmentEndpoints(CommonUtilities.AzureCloud.AzureCloudConfig.FromKnownCloudNameAsync().Result)
            };

            mockBlobApiHttpUtils = new();
            mockBlobApiHttpUtils.Setup(x => x.IsEndPointPublic(It.IsAny<Uri>()))
                .ReturnsAsync(false);

            armUrlTransformationStrategy = new ArmUrlTransformationStrategy(_ => mockBlobServiceClient.Object, options, mockBlobApiHttpUtils.Object);
            userDelegationKey = BlobsModelFactory.UserDelegationKey(Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), DateTimeOffset.UtcNow,
                DateTimeOffset.UtcNow.AddHours(1), "SIGNED_SERVICE", "V1_0", RunnerTestUtils.GenerateRandomTestAzureStorageKey());
            mockBlobServiceClient.Setup(c => c.GetUserDelegationKeyAsync(It.IsAny<DateTimeOffset?>(), It.IsAny<DateTimeOffset>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(Azure.Response.FromValue(userDelegationKey, null!));
        }

        [TestMethod]
        public async Task TransformUrlWithStrategyAsync_BlobStorageUrlWithOutPermissions_WhenRequestingRead_UrlIsReturnAsIs()
        {
            mockBlobApiHttpUtils.Setup(x => x.IsEndPointPublic(It.IsAny<Uri>()))
                .ReturnsAsync(true);

            var sourceUrl = $"https://{StorageAccountName}.blob.core.windows.net/cont/blob";
            var transformedUrl = await armUrlTransformationStrategy.TransformUrlWithStrategyAsync(sourceUrl, BlobSasPermissions.Read);

            Assert.AreEqual(1, mockBlobApiHttpUtils.Invocations.Count);
            Assert.IsNotNull(transformedUrl);
            var blobUri = new Uri(sourceUrl);
            Assert.AreEqual(blobUri.AbsoluteUri, transformedUrl.AbsoluteUri);
        }

        [TestMethod]
        public async Task TransformUrlWithStrategyAsync_BlobStorageUrlWithOutPermissions_WhenRequestingWrite_IsEndPointPublicIsNotCalled()
        {
            mockBlobApiHttpUtils.Setup(x => x.IsEndPointPublic(It.IsAny<Uri>()))
                .ReturnsAsync(true);

            var sourceUrl = $"https://{StorageAccountName}.blob.core.windows.net/cont/blob";
            var transformedUrl = await armUrlTransformationStrategy.TransformUrlWithStrategyAsync(sourceUrl, BlobSasPermissions.Create);

            Assert.IsFalse(mockBlobApiHttpUtils.Invocations.Any());
        }

        [TestMethod]
        [DataRow($"https://{StorageAccountName}.blob.core.windows.net")]
        [DataRow($"https://{StorageAccountName}.blob.core.windows.net/cont")]
        [DataRow($"https://{StorageAccountName}.blob.core.windows.net/cont/blob")]
        public async Task TransformUrlWithStrategyAsync_ValidBlobStorageUrl_SasTokenIsGenerated(string sourceUrl)
        {
            var sasTokenUrl = await armUrlTransformationStrategy.TransformUrlWithStrategyAsync(sourceUrl, BlobSasPermissions.Read);

            Assert.IsNotNull(sasTokenUrl);
            var blobUri = new Uri(sourceUrl);
            Assert.AreEqual(blobUri.Host, sasTokenUrl.Host);
            Assert.IsTrue(!string.IsNullOrEmpty(sasTokenUrl.Query));
        }

        [TestMethod]
        [DataRow($"https://storage.core.windows.net")]
        [DataRow($"https://foo.bar/cont")]
        [DataRow($"s3://foo.s3.bar")]
        public async Task TransformUrlWithStrategyAsync_InvalidBlobStorageUrl_UrlIsReturnAsIs(string sourceUrl)
        {
            var transformedUrl = await armUrlTransformationStrategy.TransformUrlWithStrategyAsync(sourceUrl, BlobSasPermissions.Read);

            Assert.IsNotNull(transformedUrl);
            var blobUri = new Uri(sourceUrl);
            Assert.AreEqual(blobUri.AbsoluteUri, transformedUrl.AbsoluteUri);
        }

        [TestMethod]
        [DataRow($"https://{StorageAccountName}.blob.core.windows.net?{SasToken}")]
        [DataRow($"https://{StorageAccountName}.blob.core.windows.net/?{SasToken}")]
        [DataRow($"https://{StorageAccountName}.blob.core.windows.net/cont?{SasToken}")]
        [DataRow($"https://{StorageAccountName}.blob.core.windows.net/cont/blob?{SasToken}")]
        public async Task TransformUrlWithStrategyAsync_BlobStorageUrlWithSasToken_UrlIsReturnAsIs(string sourceUrl)
        {
            var transformedUrl = await armUrlTransformationStrategy.TransformUrlWithStrategyAsync(sourceUrl, BlobSasPermissions.Read);

            Assert.IsNotNull(transformedUrl);
            var blobUri = new Uri(sourceUrl);
            Assert.AreEqual(blobUri.AbsoluteUri, transformedUrl.AbsoluteUri);
        }

        [TestMethod]
        public async Task TransformUrlWithStrategyAsync_CallTwiceForSameStorageAccount_CachesKey()
        {
            var sourceUrl = $"https://{StorageAccountName}.blob.core.windows.net";

            var sasTokenUrl1 = await armUrlTransformationStrategy.TransformUrlWithStrategyAsync(sourceUrl, BlobSasPermissions.Read);
            var sasTokenUrl2 = await armUrlTransformationStrategy.TransformUrlWithStrategyAsync(sourceUrl, BlobSasPermissions.Read);


            Assert.IsNotNull(sasTokenUrl1);
            Assert.IsNotNull(sasTokenUrl2);
            Assert.AreEqual(sasTokenUrl1, sasTokenUrl2);
            mockBlobServiceClient.Verify(c => c.GetUserDelegationKeyAsync(It.IsAny<DateTimeOffset?>(), It.IsAny<DateTimeOffset>(), It.IsAny<CancellationToken>()), Times.Once);
        }
    }
}
