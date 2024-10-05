// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Moq;
using Tes.Runner.Docker;
using Tes.Runner.Events;
using Tes.Runner.Logs;
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
        private Mock<ITransferOperationFactory> transferOperationFactoryMock = null!;
        private Mock<BlobDownloader> blobDownloaderMock = null!;
        private Mock<BlobUploader> blobUploaderMock = null!;

        [TestInitialize]
        public void SetUp()
        {
            dockerExecutorMock = new();
            fileOperationResolverMock = new();
            eventsPublisherMock = new();
            blobPipelineOptions = new();
            transferOperationFactoryMock = new();
            blobDownloaderMock = new();
            blobUploaderMock = new();

            transferOperationFactoryMock.Setup(f => f.CreateBlobUploaderAsync(It.IsAny<BlobPipelineOptions>()))
                .ReturnsAsync(blobUploaderMock.Object);

            transferOperationFactoryMock.Setup(f => f.CreateBlobDownloaderAsync(It.IsAny<BlobPipelineOptions>()))
                .ReturnsAsync(blobDownloaderMock.Object);

            nodeTask = new()
            {
                MountParentDirectory = "/root/parent",
                Outputs =
                [
                    new()
                    {
                        Path = "/mnt/data/output1.txt",
                        TargetUrl = "https://test.blob.core.windows.net/test/output1.txt"
                    },
                    new()
                    {
                        Path = "/*.txt",
                        TargetUrl = "https://test.blob.core.windows.net/test/output2.txt",
                    }
                ]
            };

            executor = new Executor(nodeTask, fileOperationResolverMock.Object, eventsPublisherMock.Object, transferOperationFactoryMock.Object, null!);
        }

        [TestMethod]
        public async Task UploadOutputsAsync_ResolverReturnsEmptyList_SucceedsAndReturnsZeroBytes()
        {
            fileOperationResolverMock.Setup(r => r.ResolveOutputsAsync()).ReturnsAsync([]);

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
            List<DownloadInfo> inputs =
            [
                new(FullFilePath: "/mnt/data/input1.txt", SourceUrl: new Uri("https://test.blob.core.windows.net/test/input1.txt"))
            ];

            fileOperationResolverMock.Setup(r => r.ResolveInputsAsync()).ReturnsAsync(inputs);
            var result = await executor.DownloadInputsAsync(blobPipelineOptions);
            Assert.AreEqual(Executor.ZeroBytesTransferred, result);
            eventsPublisherMock.Verify(p => p.PublishDownloadStartEventAsync(It.IsAny<NodeTask>()), Times.Once);
            eventsPublisherMock.Verify(p => p.PublishDownloadEndEventAsync(It.IsAny<NodeTask>(), 0, 0, EventsPublisher.SuccessStatus, string.Empty), Times.Once);
        }

        [TestMethod]
        public async Task DownloadInputsAsync_InputsProvided_NumberOfFilesInEndEventMatchesTheInputCount()
        {
            List<DownloadInfo> inputs =
            [
                new(FullFilePath: "/mnt/data/input1.txt", SourceUrl: new Uri("https://test.blob.core.windows.net/test/input1.txt"))
            ];

            nodeTask.Inputs =
            [
                new()
                {
                    Path = "mnt/data/input",
                    SourceUrl = "https://test.blob.core.windows.net/test/input1.txt"
                }
            ];

            blobDownloaderMock.Setup(r => r.DownloadAsync(It.IsAny<List<DownloadInfo>>()))
                .ReturnsAsync(0);

            fileOperationResolverMock.Setup(r => r.ResolveInputsAsync()).ReturnsAsync(inputs);
            var result = await executor.DownloadInputsAsync(blobPipelineOptions);
            Assert.AreEqual(Executor.ZeroBytesTransferred, result);
            eventsPublisherMock.Verify(p => p.PublishDownloadStartEventAsync(It.IsAny<NodeTask>()), Times.Once);
            eventsPublisherMock.Verify(p => p.PublishDownloadEndEventAsync(It.IsAny<NodeTask>(), nodeTask.Inputs.Count, 0, EventsPublisher.SuccessStatus, string.Empty), Times.Once);
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
            List<UploadInfo> outputs = [];
            fileOperationResolverMock.Setup(r => r.ResolveOutputsAsync()).ReturnsAsync(outputs);
            var result = await executor.UploadOutputsAsync(blobPipelineOptions);
            Assert.AreEqual(Executor.ZeroBytesTransferred, result);
            eventsPublisherMock.Verify(p => p.PublishUploadStartEventAsync(It.IsAny<NodeTask>()), Times.Once);
            eventsPublisherMock.Verify(p => p.PublishUploadEndEventAsync(It.IsAny<NodeTask>(), 0, 0, EventsPublisher.SuccessStatus, string.Empty), Times.Once);
        }

        [TestMethod]
        public async Task UploadOutputAsync_NullOptionsThrowsError_StartFailureEventsAreCreated()
        {
            await Assert.ThrowsExceptionAsync<ArgumentNullException>(() => executor.UploadOutputsAsync(null!));

            eventsPublisherMock.Verify(p => p.PublishUploadStartEventAsync(It.IsAny<NodeTask>()), Times.Once);
            eventsPublisherMock.Verify(p => p.PublishUploadEndEventAsync(It.IsAny<NodeTask>(), 0, 0, EventsPublisher.FailedStatus, It.Is<string?>((c) => !string.IsNullOrEmpty(c))), Times.Once);
        }

        [TestMethod]
        public async Task ExecuteNodeContainerTaskAsync_SuccessfulExecution_ReturnsContainerResult()
        {
            dockerExecutorMock.Setup(d => d.RunOnContainerAsync(It.IsAny<ExecutionOptions>(), It.IsAny<Func<string, Task<IStreamLogReader>>>()))
                .ReturnsAsync(new ContainerExecutionResult("taskId", Error: string.Empty, ExitCode: 0));

            var result = await executor.ExecuteNodeContainerTaskAsync(dockerExecutorMock.Object);

            Assert.AreEqual(0, result.ContainerResult.ExitCode);
            Assert.AreEqual(string.Empty, result.ContainerResult.Error);
        }

        [TestMethod]
        public async Task ExecuteNodeContainerTaskAsync_ExecutionFails_ReturnsContainerResult()
        {

            dockerExecutorMock.Setup(d => d.RunOnContainerAsync(It.IsAny<ExecutionOptions>(), It.IsAny<Func<string, Task<IStreamLogReader>>>()))
                .ReturnsAsync(new ContainerExecutionResult("taskId", Error: "Error", ExitCode: 1));

            var result = await executor.ExecuteNodeContainerTaskAsync(dockerExecutorMock.Object);

            Assert.AreEqual(1, result.ContainerResult.ExitCode);
            Assert.AreEqual("Error", result.ContainerResult.Error);
        }

        [TestMethod]
        public async Task ExecuteNodeContainerTaskAsync_SuccessfulExecution_StartAndSuccessEventsArePublished()
        {
            dockerExecutorMock.Setup(d => d.RunOnContainerAsync(It.IsAny<ExecutionOptions>(), It.IsAny<Func<string, Task<IStreamLogReader>>>()))
                .ReturnsAsync(new ContainerExecutionResult("taskId", Error: string.Empty, ExitCode: 0));

            await executor.ExecuteNodeContainerTaskAsync(dockerExecutorMock.Object);

            eventsPublisherMock.Verify(p => p.PublishExecutorStartEventAsync(It.IsAny<NodeTask>()), Times.Once);
            eventsPublisherMock.Verify(p => p.PublishExecutorEndEventAsync(It.IsAny<NodeTask>(), 0, EventsPublisher.SuccessStatus, string.Empty), Times.Once);
        }

        [TestMethod]
        public async Task ExecuteNodeContainerTaskAsync_ExecutionFails_StartAndFailureEventsArePublished()
        {
            dockerExecutorMock.Setup(d => d.RunOnContainerAsync(It.IsAny<ExecutionOptions>(), It.IsAny<Func<string, Task<IStreamLogReader>>>()))
                .ReturnsAsync(new ContainerExecutionResult("taskId", Error: "Error", ExitCode: 1));

            await executor.ExecuteNodeContainerTaskAsync(dockerExecutorMock.Object);

            eventsPublisherMock.Verify(p => p.PublishExecutorStartEventAsync(It.IsAny<NodeTask>()), Times.Once);
            eventsPublisherMock.Verify(p => p.PublishExecutorEndEventAsync(It.IsAny<NodeTask>(), 1, EventsPublisher.FailedStatus, "Error"), Times.Once);
        }

        [TestMethod]
        public async Task ExecuteNodeContainerTaskAsync_ExecutionThrows_StartAndFailureEventsArePublished()
        {
            dockerExecutorMock.Setup(d => d.RunOnContainerAsync(It.IsAny<ExecutionOptions>(), It.IsAny<Func<string, Task<IStreamLogReader>>>()))
                .ThrowsAsync(new Exception("Error"));

            await Assert.ThrowsExceptionAsync<Exception>(async () => await executor.ExecuteNodeContainerTaskAsync(dockerExecutorMock.Object));

            eventsPublisherMock.Verify(p => p.PublishExecutorStartEventAsync(It.IsAny<NodeTask>()), Times.Once);
            eventsPublisherMock.Verify(p => p.PublishExecutorEndEventAsync(It.IsAny<NodeTask>(), Executor.DefaultErrorExitCode, EventsPublisher.FailedStatus, "Error"), Times.Once);
        }
    }
}
