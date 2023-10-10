// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Blobs;
using Tes.Runner.Logs;
using Tes.Runner.Transfer;

namespace Tes.Runner.Test.Logs
{
    [TestClass, TestCategory("Unit")]
    public class AppendBlobLogPublisherTests
    {
        private AppendBlobLogPublisher publisher = null!;
        private readonly string targetUrl = "https://test.blob.core.windows.net/cont?sig=signature";
        private readonly string logNamePrefix = "prefix";

        [DataTestMethod]
        [DataRow(0, "prefix_stdout", "prefix_stdout.txt")]
        [DataRow(BlobSizeUtils.MaxBlobBlocksCount + 1, "prefix_stdout", "prefix_stdout_1.txt")]
        [DataRow(2 * BlobSizeUtils.MaxBlobBlocksCount + 1, "prefix_stdout", "prefix_stdout_2.txt")]
        public void GetUriAndBlobNameFromCurrentState_InitialState_ReturnsExpectedUrl(int currentBlockCount, string baseLogName, string expectedBlobName)
        {
            publisher = new AppendBlobLogPublisher(targetUrl, logNamePrefix);

            var blobBuilder = new BlobUriBuilder(new Uri(targetUrl)) { BlobName = expectedBlobName };

            var uri = publisher.GetUriAndBlobNameFromCurrentState(currentBlockCount, baseLogName, out var blobLogName);
            Assert.AreEqual(blobBuilder.ToUri().ToString(), uri.ToString());
            Assert.AreEqual(expectedBlobName, blobLogName);
        }

        [TestMethod]
        public void GetUriAndBlobNameFromCurrentState_TargetUrlWithSegments_ReturnsUrlWithSegmentsAndBlobName()
        {
            var targetUrlWithSegments = "https://test.blob.core.windows.net/cont/seg1/seg2?sig=signature";
            publisher = new AppendBlobLogPublisher(targetUrlWithSegments, logNamePrefix);

            var blobBuilder = new BlobUriBuilder(new Uri(targetUrlWithSegments));
            blobBuilder.BlobName += "/prefix_stdout.txt";

            var uri = publisher.GetUriAndBlobNameFromCurrentState(0, "prefix_stdout", out var blobLogName);

            Assert.AreEqual(blobBuilder.ToUri().ToString(), uri.ToString());
            Assert.AreEqual("prefix_stdout.txt", blobLogName);
        }
    }
}
