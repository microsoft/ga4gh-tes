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
    public class BlobDownloaderTest
    {
        private BlobContainerClient blobContainerClient;
        private Guid containerId;
        private BlobDownloader blobDownloader;
        private readonly BlobPipelineOptions blobPipelineOptions = new BlobPipelineOptions();

        [TestInitialize]
        public async Task Init()
        {
            containerId = Guid.NewGuid();
            var options = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2020_12_06);

            var blobService = new BlobServiceClient("UseDevelopmentStorage=true", options);

            blobContainerClient = blobService.GetBlobContainerClient(containerId.ToString());

            blobContainerClient.Create(PublicAccessType.None);

            blobDownloader = new BlobDownloader(blobPipelineOptions,
                await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(10, blobPipelineOptions.BlockSize));
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
        public async Task DownloadAsync_DownloadsFilesAndChecksumMatches(int numberOfMiB, int extraBytes)
        {
            var sourceFilename = await RunnerTestUtils.CreateTempFileWithContentAsync(numberOfMiB, extraBytes);
            var blobClient = blobContainerClient.GetBlobClient(sourceFilename);

            // Uploads a file.
            await using var fileToUpload = File.OpenRead(sourceFilename);
            await blobClient.UploadAsync(fileToUpload);

            var url = CreateSasUrl(blobClient, sourceFilename);

            var downloadFilename = sourceFilename + "_down";

            await blobDownloader.DownloadAsync(new List<DownloadInfo>() { new DownloadInfo(downloadFilename, url) });

            Assert.AreEqual(RunnerTestUtils.CalculateMd5(sourceFilename),
                RunnerTestUtils.CalculateMd5(downloadFilename));
        }

        private Uri CreateSasUrl(BlobClient blobClient, string file)
        {
            var sasBuilder = new BlobSasBuilder(BlobContainerSasPermissions.All, DateTimeOffset.UtcNow.AddHours(1))
            {
                BlobContainerName = blobClient.GetParentBlobContainerClient().Name,
                BlobName = file,
                Resource = "b"
            };

            var url = blobContainerClient.GetBlobClient(file).GenerateSasUri(sasBuilder);
            return url;
        }
    }
}
