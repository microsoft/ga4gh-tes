using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using Tes.Runner.Transfer;

namespace Tes.Runner.Test
{
    [TestClass]
    [TestCategory("Integration")]
    public class BlobUploaderTest
    {
        private BlobContainerClient blobContainerClient;
        private Guid containerId;
        private BlobUploader blobUploader;
        private int blockSize = 10 * Units.MiB;

        [TestInitialize]
        public async Task Init()
        {
            containerId = Guid.NewGuid();
            var options = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2020_12_06);

            var blobService = new BlobServiceClient("UseDevelopmentStorage=true", options);

            blobContainerClient = blobService.GetBlobContainerClient(containerId.ToString());

            blobContainerClient.Create(PublicAccessType.None);

            blobUploader = new BlobUploader(new BlobPipelineOptions(blockSize, 10, 10, 10),
                await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(10, blockSize));
        }

        [TestCleanup]
        public void Cleanup()
        {
            blobContainerClient.DeleteIfExists();
        }
        [TestMethod]
        public async Task UploadFile_10MiBFile_UploadsSuccessfully()
        {
            var file = await RunnerTestUtils.CreateTempFileWithContentAsync(10);
            var blobClient = blobContainerClient.GetBlobClient(file);

            // Create a SAS token that's valid for one hour.
            var sasBuilder = new BlobSasBuilder(BlobContainerSasPermissions.All, DateTimeOffset.UtcNow.AddHours(1))
            {
                BlobContainerName = blobClient.GetParentBlobContainerClient().Name,
                BlobName = blobClient.Name,
                Resource = "b"
            };

            var url = blobContainerClient.GetBlobClient(file).GenerateSasUri(sasBuilder);

            await blobUploader.UploadAsync(new List<UploadInfo>() { new UploadInfo(file, url) });

            var blobProperties = await blobClient.GetPropertiesAsync();
            Assert.IsNotNull(blobProperties);
            Assert.AreEqual(10 * Units.MiB, blobProperties.Value.ContentLength);
        }
    }
}
