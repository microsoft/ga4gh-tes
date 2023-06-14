// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;
using Moq;
using Tes.Runner.Transfer;

namespace Tes.Runner.Test
{
    [TestClass]
    [TestCategory("Unit")]
    public class ProcessedPartsProcessorTests
    {
        private ProcessedPartsProcessor? processedPartsProcessor;
        private Mock<IBlobPipeline>? pipeline;
        private Channel<ProcessedBuffer>? processedBuffer;
        private Channel<PipelineBuffer>? readBuffer;

        [TestInitialize]
        public void SetUp()
        {
            pipeline = new Mock<IBlobPipeline>();
            processedBuffer = Channel.CreateUnbounded<ProcessedBuffer>();
            readBuffer = Channel.CreateUnbounded<PipelineBuffer>();
            processedPartsProcessor = new ProcessedPartsProcessor(pipeline.Object);
        }

        [DataTestMethod]
        [DataRow(1, BlobSizeUtils.MiB * 10, 10)]
        [DataRow(10, BlobSizeUtils.MiB * 10, 10)]
        [DataRow(1, 0, 1)]
        public async Task StartProcessedPartsProcessorAsync_CallsOnCompleteOnceForEachFileAndProccessedBufferIsEmpty(int expectedNumberOfFiles, long fileSize, int numberOfPartsPerFile)
        {
            processedPartsProcessor = new ProcessedPartsProcessor(pipeline!.Object);

            for (var f = 0; f < expectedNumberOfFiles; f++)
            {
                await RunnerTestUtils.AddProcessedBufferAsync(processedBuffer!, $"file{f}", numberOfPartsPerFile,
                    fileSize);
            };

            await processedPartsProcessor!.StartProcessedPartsProcessorAsync(expectedNumberOfFiles, processedBuffer!, readBuffer!);

            processedBuffer!.Writer.Complete();

            pipeline!.Verify(p => p.OnCompletionAsync(It.IsAny<long>(), It.IsAny<Uri?>(), It.IsAny<string>(), It.IsAny<string>()), Times.Exactly(expectedNumberOfFiles));

            var parts = await RunnerTestUtils.ReadAllPipelineBuffersAsync(processedBuffer!.Reader.ReadAllAsync());

            Assert.IsNotNull(parts);
            Assert.AreEqual(0, parts.Count);
        }

    }
}
