// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging.Abstractions;
using Tes.Runner.Logs;
using Tes.Runner.Transfer;

namespace Tes.Runner.Test.Logs
{
    [TestClass, TestCategory("Unit")]
    public class AppendBlobLogPublisherTests
    {
        private AppendBlobLogPublisher publisher = null!;
        private readonly string logNamePrefix = "prefix";

        [DataTestMethod]
        [DataRow(0, "prefix_stdout", "prefix_stdout.txt")]
        [DataRow(BlobSizeUtils.MaxBlobBlocksCount + 1, "prefix_stdout", "prefix_stdout_1.txt")]
        [DataRow(2 * BlobSizeUtils.MaxBlobBlocksCount + 1, "prefix_stdout", "prefix_stdout_2.txt")]
        public void GetUriAndBlobNameFromCurrentState_InitialState_ReturnsExpectedUrl(int currentBlockCount, string baseLogName, string expectedBlobName)
        {
            Uri targetUrl = new("https://test.blob.core.windows.net/cont?sig=signature");
            publisher = new AppendBlobLogPublisher(targetUrl, new(new(), logger => HttpRetryPolicyDefinition.DefaultAsyncRetryPolicy(logger), NullLogger<BlobApiHttpUtils>.Instance), logNamePrefix, NullLogger<AppendBlobLogPublisher>.Instance);

            var blobBuilder = new BlobUriBuilder(targetUrl) { BlobName = expectedBlobName };

            var uri = publisher.GetUriAndBlobNameFromCurrentState(currentBlockCount, baseLogName, out var blobLogName);
            Assert.AreEqual(blobBuilder.ToUri().ToString(), uri.ToString());
            Assert.AreEqual(expectedBlobName, blobLogName);
        }

        [TestMethod]
        public void GetUriAndBlobNameFromCurrentState_TargetUrlWithSegments_ReturnsUrlWithSegmentsAndBlobName()
        {
            Uri targetUrlWithSegments = new("https://test.blob.core.windows.net/cont/seg1/seg2?sig=signature");
            publisher = new AppendBlobLogPublisher(targetUrlWithSegments, new(new(), logger => HttpRetryPolicyDefinition.DefaultAsyncRetryPolicy(logger), NullLogger<BlobApiHttpUtils>.Instance), logNamePrefix, NullLogger<AppendBlobLogPublisher>.Instance);

            var blobBuilder = new BlobUriBuilder(targetUrlWithSegments);
            blobBuilder.BlobName += "/prefix_stdout.txt";

            var uri = publisher.GetUriAndBlobNameFromCurrentState(0, "prefix_stdout", out var blobLogName);

            Assert.AreEqual(blobBuilder.ToUri().ToString(), uri.ToString());
            Assert.AreEqual("prefix_stdout.txt", blobLogName);
        }
    }
}
