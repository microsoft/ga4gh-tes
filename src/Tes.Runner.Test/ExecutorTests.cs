// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Moq;
using Tes.Runner.Events;
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
        private Mock<EventsPublisher> eventsPublisherMock = null!;

        [TestInitialize]
        public void SetUp()
        {
            fileOperationResolverMock = new Mock<FileOperationResolver>();
            eventsPublisherMock = new Mock<EventsPublisher>();
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
                        Path = "/*.txt",
                        TargetUrl = "https://test.blob.core.windows.net/test/output2.txt",
                    }
                }
            };
            executor = new Executor(nodeTask, fileOperationResolverMock.Object, eventsPublisherMock.Object);
        }

        [TestMethod]
        public async Task UploadOutputsAsync_ResolverReturnsEmptyList_SucceedsAndReturnsZeroBytes()
        {
            fileOperationResolverMock.Setup(r => r.ResolveOutputsAsync()).ReturnsAsync(new List<UploadInfo>());

            var result = await executor.UploadOutputsAsync(blobPipelineOptions);

            Assert.AreEqual(Executor.ZeroBytesTransferred, result);
        }

        [TestMethod]
        public async Task UploadOutputsAsync_ResolverReturnsNull_SucceedsAndReturnsZeroBytes()
        {
            fileOperationResolverMock.Setup(r => r.ResolveOutputsAsync()).ReturnsAsync((List<UploadInfo>?)null);
            var result = await executor.UploadOutputsAsync(blobPipelineOptions);
            Assert.AreEqual(Executor.ZeroBytesTransferred, result);
        }

        [TestMethod]
        public async Task DownloadInputsAsync_InputProvided_StartSuccessEventsAreCreated()
        {
            var inputs = new List<DownloadInfo>
            {
                new DownloadInfo( FullFilePath: "/mnt/data/input1.txt", SourceUrl: new Uri("https://test.blob.core.windows.net/test/input1.txt"))
            };

            fileOperationResolverMock.Setup(r => r.ResolveInputsAsync()).ReturnsAsync(inputs);
            var result = await executor.DownloadInputsAsync(blobPipelineOptions);
            Assert.AreEqual(Executor.ZeroBytesTransferred, result);
            eventsPublisherMock.Verify(p => p.PublishDownloadEventStartAsync(It.IsAny<NodeTask>()), Times.Once);
            eventsPublisherMock.Verify(p => p.PublishDownloadEventEndAsync(It.IsAny<NodeTask>(), 0, 0, EventsPublisher.SuccessStatus, string.Empty), Times.Once);
        }

        [TestMethod]
        public async Task DownloadInputsAsync_ParameterIsNullAndThrows_StartFailureEventsAreCreated()
        {
            var inputs = new List<DownloadInfo>
            {
                new DownloadInfo( FullFilePath: "/mnt/data/input1.txt", SourceUrl: new Uri("https://test.blob.core.windows.net/test/input1.txt"))
            };

            await Assert.ThrowsExceptionAsync<ArgumentNullException>(() => executor.DownloadInputsAsync(null));
            eventsPublisherMock.Verify(p => p.PublishDownloadEventStartAsync(It.IsAny<NodeTask>()), Times.Once);
            eventsPublisherMock.Verify(p => p.PublishDownloadEventEndAsync(It.IsAny<NodeTask>(), 0, 0, EventsPublisher.FailedStatus, It.Is<string?>((c) => !string.IsNullOrEmpty(c))), Times.Once);
        }
    }
}
