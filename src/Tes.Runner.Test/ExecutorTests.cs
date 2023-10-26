// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Moq;
using Tes.Runner.Docker;
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
        private Mock<DockerExecutor> dockerExecutorMock = null!;

        [TestInitialize]
        public void SetUp()
        {
            dockerExecutorMock = new Mock<DockerExecutor>();
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
            eventsPublisherMock.Verify(p => p.PublishDownloadStartEventAsync(It.IsAny<NodeTask>()), Times.Once);
            eventsPublisherMock.Verify(p => p.PublishDownloadEndEventAsync(It.IsAny<NodeTask>(), 0, 0, EventsPublisher.SuccessStatus, string.Empty), Times.Once);
        }

        [TestMethod]
        public async Task DownloadInputsAsync_ParameterIsNullAndThrows_StartFailureEventsAreCreated()
        {
            await Assert.ThrowsExceptionAsync<ArgumentNullException>(() => executor.DownloadInputsAsync(null!));
            eventsPublisherMock.Verify(p => p.PublishDownloadStartEventAsync(It.IsAny<NodeTask>()), Times.Once);
            eventsPublisherMock.Verify(p => p.PublishDownloadEndEventAsync(It.IsAny<NodeTask>(), 0, 0, EventsPublisher.FailedStatus, It.Is<string?>((c) => !string.IsNullOrEmpty(c))), Times.Once);
        }

        [TestMethod]
        public async Task UploadOutputsAsync_NoOutputProvided_StartSuccessEventsAreCreated()
        {
            var outputs = new List<UploadInfo>();
            fileOperationResolverMock.Setup(r => r.ResolveOutputsAsync()).ReturnsAsync(outputs);
            var result = await executor.UploadOutputsAsync(blobPipelineOptions);
            Assert.AreEqual(Executor.ZeroBytesTransferred, result);
            eventsPublisherMock.Verify(p => p.PublishUploadStartEventAsync(It.IsAny<NodeTask>()), Times.Once);
            eventsPublisherMock.Verify(p => p.PublishUploadEndEventAsync(It.IsAny<NodeTask>(), 0, 0, EventsPublisher.SuccessStatus, string.Empty, null), Times.Once);
        }

        [TestMethod]
        public async Task UploadOutputAsync_NullOptionsThrowsError_StartFailureEventsAreCreated()
        {
            await Assert.ThrowsExceptionAsync<ArgumentNullException>(() => executor.UploadOutputsAsync(null!));

            eventsPublisherMock.Verify(p => p.PublishUploadStartEventAsync(It.IsAny<NodeTask>()), Times.Once);
            eventsPublisherMock.Verify(p => p.PublishUploadEndEventAsync(It.IsAny<NodeTask>(), 0, 0, EventsPublisher.FailedStatus, It.Is<string?>((c) => !string.IsNullOrEmpty(c)), null), Times.Once);
        }

        [TestMethod]
        public async Task ExecuteNodeContainerTaskAsync_SuccessfulExecution_ReturnsContainerResult()
        {
            dockerExecutorMock.Setup(d => d.RunOnContainerAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<List<string>>(), It.IsAny<List<string>>(), It.IsAny<string?>()))
                .ReturnsAsync(new ContainerExecutionResult("taskId", Error: string.Empty, ExitCode: 0));

            var result = await executor.ExecuteNodeContainerTaskAsync(dockerExecutorMock.Object);

            Assert.AreEqual(0, result.ContainerResult.ExitCode);
            Assert.AreEqual(string.Empty, result.ContainerResult.Error);
        }

        [TestMethod]
        public async Task ExecuteNodeContainerTaskAsync_ExecutionFails_ReturnsContainerResult()
        {

            dockerExecutorMock.Setup(d => d.RunOnContainerAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<List<string>>(), It.IsAny<List<string>>(), It.IsAny<string?>()))
                .ReturnsAsync(new ContainerExecutionResult("taskId", Error: "Error", ExitCode: 1));

            var result = await executor.ExecuteNodeContainerTaskAsync(dockerExecutorMock.Object);

            Assert.AreEqual(1, result.ContainerResult.ExitCode);
            Assert.AreEqual("Error", result.ContainerResult.Error);
        }

        [TestMethod]
        public async Task ExecuteNodeContainerTaskAsync_SuccessfulExecution_StartAndSuccessEventsArePublished()
        {
            dockerExecutorMock.Setup(d => d.RunOnContainerAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<List<string>>(), It.IsAny<List<string>>(), It.IsAny<string?>()))
                .ReturnsAsync(new ContainerExecutionResult("taskId", Error: string.Empty, ExitCode: 0));

            await executor.ExecuteNodeContainerTaskAsync(dockerExecutorMock.Object);

            eventsPublisherMock.Verify(p => p.PublishExecutorStartEventAsync(It.IsAny<NodeTask>()), Times.Once);
            eventsPublisherMock.Verify(p => p.PublishExecutorEndEventAsync(It.IsAny<NodeTask>(), 0, EventsPublisher.SuccessStatus, string.Empty), Times.Once);
        }

        [TestMethod]
        public async Task ExecuteNodeContainerTaskAsync_ExecutionFails_StartAndFailureEventsArePublished()
        {
            dockerExecutorMock.Setup(d => d.RunOnContainerAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<List<string>>(), It.IsAny<List<string>>(), It.IsAny<string?>()))
                .ReturnsAsync(new ContainerExecutionResult("taskId", Error: "Error", ExitCode: 1));

            await executor.ExecuteNodeContainerTaskAsync(dockerExecutorMock.Object);

            eventsPublisherMock.Verify(p => p.PublishExecutorStartEventAsync(It.IsAny<NodeTask>()), Times.Once);
            eventsPublisherMock.Verify(p => p.PublishExecutorEndEventAsync(It.IsAny<NodeTask>(), 1, EventsPublisher.FailedStatus, "Error"), Times.Once);
        }

        [TestMethod]
        public async Task ExecuteNodeContainerTaskAsync_ExecutionThrows_StartAndFailureEventsArePublished()
        {
            dockerExecutorMock.Setup(d => d.RunOnContainerAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<List<string>>(), It.IsAny<List<string>>(), It.IsAny<string?>()))
                .ThrowsAsync(new Exception("Error"));

            await Assert.ThrowsExceptionAsync<Exception>(async () => await executor.ExecuteNodeContainerTaskAsync(dockerExecutorMock.Object));

            eventsPublisherMock.Verify(p => p.PublishExecutorStartEventAsync(It.IsAny<NodeTask>()), Times.Once);
            eventsPublisherMock.Verify(p => p.PublishExecutorEndEventAsync(It.IsAny<NodeTask>(), Executor.DefaultErrorExitCode, EventsPublisher.FailedStatus, "Error"), Times.Once);
        }
    }
}
