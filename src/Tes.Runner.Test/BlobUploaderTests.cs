// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using Tes.Runner.Transfer;

namespace Tes.Runner.Test
{
    [TestClass]
    [TestCategory("Integration")]
    [Ignore]
    public class BlobUploaderTests
    {
#pragma warning disable CS8618
        private BlobContainerClient blobContainerClient;
        private Guid containerId;
        private BlobUploader blobUploader;
        private readonly BlobPipelineOptions blobPipelineOptions = new BlobPipelineOptions();
#pragma warning restore CS8618

        [TestInitialize]
        public async Task Init()
        {
            containerId = Guid.NewGuid();
            var options = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2020_12_06);

            var blobService = new BlobServiceClient("UseDevelopmentStorage=true", options);

            blobContainerClient = blobService.GetBlobContainerClient(containerId.ToString());

            await blobContainerClient.CreateAsync(PublicAccessType.None);

            blobUploader = new BlobUploader(blobPipelineOptions,
                await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(10, blobPipelineOptions.BlockSizeBytes));
        }

        [TestCleanup]
        public void Cleanup()
        {
            blobContainerClient.DeleteIfExists();
        }
        [DataTestMethod]
        [DataRow(10, 0)]
        [DataRow(10, 100)]
        [DataRow(100, 0)]
        [DataRow(99, 1)]
        [DataRow(100, 1)]
        [DataRow(0, 0)]
        public async Task UploadFile_LocalFileUploadsAndMatchesSize(int numberOfMiB, int extraBytes)
        {
            var file = await RunnerTestUtils.CreateTempFileWithContentAsync(numberOfMiB, extraBytes);
            var blobClient = blobContainerClient.GetBlobClient(file);

            // Create a SAS token that's valid for one hour.
            var url = CreateSasUrl(blobClient, file);

            await blobUploader.UploadAsync(new List<UploadInfo>() { new UploadInfo(file, url) });

            var blobProperties = await blobClient.GetPropertiesAsync();
            var fileSize = (numberOfMiB * BlobSizeUtils.MiB) + extraBytes;
            Assert.IsNotNull(blobProperties);
            Assert.AreEqual(fileSize, blobProperties.Value.ContentLength);
        }

        private Uri CreateSasUrl(BlobClient blobClient, string file)
        {
            var sasBuilder = new BlobSasBuilder(BlobContainerSasPermissions.All, DateTimeOffset.UtcNow.AddHours(1))
            {
                BlobContainerName = blobClient.GetParentBlobContainerClient().Name,
                BlobName = blobClient.Name,
                Resource = "b"
            };

            var url = blobContainerClient.GetBlobClient(file).GenerateSasUri(sasBuilder);
            return url;
        }
    }
}
