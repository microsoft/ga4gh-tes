// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using Microsoft.Extensions.Logging.Abstractions;
using Tes.Runner.Transfer;

namespace Tes.Runner.Test
{
    [TestClass]
    [TestCategory("Integration")]
    [Ignore]
    public class BlobUploaderTests
    {
        private BlobContainerClient blobContainerClient = null!;
        private Guid containerId;
        private BlobUploader blobUploader = null!;
        private readonly BlobPipelineOptions blobPipelineOptions = new(CalculateFileContentMd5: true);

        [TestInitialize]
        public async Task Init()
        {
            containerId = Guid.NewGuid();
            var options = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2020_12_06);

            var blobService = new BlobServiceClient("UseDevelopmentStorage=true", options);

            blobContainerClient = blobService.GetBlobContainerClient(containerId.ToString());

            await blobContainerClient.CreateAsync(PublicAccessType.None);

            blobUploader = new(blobPipelineOptions, new(new(), logger => HttpRetryPolicyDefinition.DefaultAsyncRetryPolicy(logger), NullLogger<BlobApiHttpUtils>.Instance),
                await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(10, blobPipelineOptions.BlockSizeBytes),
                pipeline => new(pipeline, NullLogger<ProcessedPartsProcessor>.Instance),
                (pipeline, options) => new(pipeline, options, NullLogger<PartsProducer>.Instance),
                (pipeline, options, channel, strategy) => new(pipeline, options, channel, strategy, NullLogger<PartsWriter>.Instance),
                (pipeline, options, channel, strategy) => new(pipeline, options, channel, strategy, NullLogger<PartsReader>.Instance),
                NullLogger<BlobUploader>.Instance);
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

            await blobUploader.UploadAsync([new(file, url)]);

            var blobProperties = await blobClient.GetPropertiesAsync();
            var fileSize = (numberOfMiB * BlobSizeUtils.MiB) + extraBytes;
            Assert.IsNotNull(blobProperties);
            Assert.AreEqual(fileSize, blobProperties.Value.ContentLength);
        }

        [TestMethod]
        public async Task UploadFile_ContentMD5IsSet()
        {
            var file = await RunnerTestUtils.CreateTempFileWithContentAsync(numberOfMiB: 1, extraBytes: 0);
            var blobClient = blobContainerClient.GetBlobClient(file);

            // Create a SAS token that's valid for one hour.
            var url = CreateSasUrl(blobClient, file);

            await blobUploader.UploadAsync([new(file, url)]);

            var blobProperties = await blobClient.GetPropertiesAsync();

            Assert.IsNotNull(blobProperties);
            Assert.IsNotNull(blobProperties.Value.ContentHash);

            var contentHashBytes = Convert.FromBase64String(Convert.ToBase64String(blobProperties.Value.ContentHash));
            var contentHash = Encoding.UTF8.GetString(contentHashBytes);

            var fileHash = RunnerTestUtils.CalculateMd5(file);

            Assert.AreEqual(fileHash, contentHash);
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
