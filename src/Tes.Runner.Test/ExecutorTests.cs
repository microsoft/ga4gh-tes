// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Moq;
using Tes.Runner.Models;
using Tes.Runner.Storage;
using Tes.Runner.Transfer;

namespace Tes.Runner.Test
{
    [TestClass, TestCategory("Unit")]
    public class ExecutorTests
    {
        private Executor executor = null!;
        private Mock<FileOperationResolver> fileOperationResolverMock = null!;
        private NodeTask nodeTask = null!;
        private BlobPipelineOptions blobPipelineOptions = null!;

        [TestInitialize]
        public void SetUp()
        {
            fileOperationResolverMock = new Mock<FileOperationResolver>();
            blobPipelineOptions = new BlobPipelineOptions();
            nodeTask = new NodeTask()
            {
                Outputs = new List<FileOutput>
                {
                    new FileOutput()
                    {
                        Path = "/mnt/data/output1.txt",
                        TargetUrl = "https://test.blob.core.windows.net/test/output1.txt"
                    },
                    new FileOutput()
                    {
                        Path = "*.txt",
                        TargetUrl = "https://test.blob.core.windows.net/test/output2.txt",
                        PathPrefix = "/mnt/data"
                    }
                }
            };
            executor = new Executor(nodeTask, fileOperationResolverMock.Object);
        }

        [TestMethod]
        public async Task UploadOutputsAsyncTest_ResolverReturnsEmptyList_SucceedsAndReturnsZeroBytes()
        {
            fileOperationResolverMock.Setup(r => r.ResolveOutputsAsync()).ReturnsAsync(new List<UploadInfo>());

            var result = await executor.UploadOutputsAsync(blobPipelineOptions);

            Assert.AreEqual(Executor.ZeroBytesTransferred, result);
        }
    }
}
