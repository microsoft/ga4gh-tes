// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Moq;
using Tes.Runner.Transfer;

namespace Tes.Runner.Test.Transfer
{
    [TestClass]
    public class PartsProcessorTests
    {
        private Channel<PipelineBuffer> readChannel = null!;
        private TestPartsProcessor partsProcessor = null!;
        private readonly int blockSizeBytes = BlobSizeUtils.DefaultBlockSizeBytes;
        private readonly long fileSize = BlobSizeUtils.MiB * 100;
        private readonly string fileName = "tempFile";
        private readonly string blobUri = "https://foo.bar/cont/blob";
        private readonly Random random = new();
        private Mock<IScalingStrategy> strategyMock = null!;

        [TestInitialize]
        public void SetUp()
        {
            readChannel = Channel.CreateBounded<PipelineBuffer>(RunnerTestUtils.PipelineBufferCapacity);
            strategyMock = new Mock<IScalingStrategy>();
            strategyMock.Setup(s => s.GetScalingDelay(It.IsAny<int>())).Returns(TimeSpan.FromMilliseconds(10));
            strategyMock.Setup(s => s.IsScalingAllowed(It.IsAny<int>(), It.IsAny<TimeSpan>())).Returns(true);
            partsProcessor = new TestPartsProcessor(new Mock<IBlobPipeline>().Object, new BlobPipelineOptions(),
                               Channel.CreateBounded<byte[]>(RunnerTestUtils.MemBuffersCapacity), strategyMock.Object);


        }

        [DataTestMethod]
        [DataRow(1)]
        [DataRow(10)]
        [DataRow(100)]
        public async Task StartProcessors_NProcessorsAnd1Fail_CancellationHappens(int numOfProcessors)
        {
            await PrepareReaderChannelAsync();

            //throw on processing a random part
            partsProcessor.SetThrowOnOrdinal(random.Next(0, readChannel.Reader.Count - 1));

            Exception? exception = null;
            try
            {
                await partsProcessor.StartProcessors(numOfProcessors, readChannel);
            }
            catch (Exception e)
            {
                exception = e;
            }

            Assert.IsNotNull(exception);
            Assert.IsTrue(partsProcessor.CancellationTokenArg.IsCancellationRequested);
        }

        [DataTestMethod]
        [DataRow(1)]
        [DataRow(10)]
        [DataRow(100)]
        public async Task StartProcessors_NProcessorsNoFailure_ProcessAllPartsNoCancellation(int numOfProcessors)
        {
            await PrepareReaderChannelAsync();
            var expectedCount = readChannel.Reader.Count;

            await partsProcessor.StartProcessors(numOfProcessors, readChannel);

            Assert.AreEqual(expectedCount, partsProcessor.ProcessedCount);
            Assert.IsFalse(partsProcessor.CancellationTokenArg.IsCancellationRequested);
        }

        private async Task PrepareReaderChannelAsync()
        {
            await RunnerTestUtils.PreparePipelineChannelAsync(blockSizeBytes, fileSize, fileName, blobUri, readChannel);
        }

        [DataTestMethod]
        [DataRow(1)]
        [DataRow(10)]
        public async Task StartProcessors_0ProcessingTime_ScalingStrategyIsCalledAtLeastOnce(int numOfProcessors)
        {
            await PrepareReaderChannelAsync();
            await partsProcessor.StartProcessors(numOfProcessors, readChannel);

            //since the part processing time ~0, a single processor should be sufficient, therefore the strategy should be called at least once
            strategyMock.Verify(s => s.GetScalingDelay(It.IsAny<int>()), Times.AtLeastOnce);
            strategyMock.Verify(s => s.IsScalingAllowed(It.IsAny<int>(), It.IsAny<TimeSpan>()), Times.AtLeastOnce);
        }

        [TestMethod]
        public async Task StartProcessors_1secPartProcessingTime_ScalingStrategyIsCalledMoreThanOnce()
        {
            await PrepareReaderChannelAsync();
            partsProcessor.SetDelayOnProcessing(TimeSpan.FromSeconds(1));

            strategyMock.Setup(s => s.GetScalingDelay(It.IsAny<int>())).Returns(TimeSpan.FromSeconds(1));
            await partsProcessor.StartProcessors(10, readChannel);
            //since the part processing time ~1sec, multiple prcoessors should be required, therefore the strategy should be called more than once
            strategyMock.Verify(s => s.GetScalingDelay(It.IsAny<int>()), Times.AtLeast(2));
            strategyMock.Verify(s => s.IsScalingAllowed(It.IsAny<int>(), It.IsAny<TimeSpan>()), Times.AtLeast(2));
        }
    }

    internal class TestPartsProcessor : PartsProcessor
    {
        internal PipelineBuffer BufferArg { get; private set; } = null!;
        internal CancellationToken CancellationTokenArg { get; private set; }
        private readonly SemaphoreSlim semaphore = new(1, 1);
        private readonly CancellationTokenSource cancellationTokenSource = new();
        internal int ProcessedCount { get; private set; }
        private int? throwOnOrdinal;
        private TimeSpan? partDelay;

        internal TestPartsProcessor(IBlobPipeline blobPipeline, BlobPipelineOptions blobPipelineOptions,
            Channel<byte[]> memoryBufferChannel, IScalingStrategy scalingStrategy) : base(blobPipeline, blobPipelineOptions, memoryBufferChannel, scalingStrategy, Microsoft.Extensions.Logging.Abstractions.NullLogger<TestPartsProcessor>.Instance)
        {
        }

        internal void SetThrowOnOrdinal(int ordinal)
        {
            throwOnOrdinal = ordinal;
        }

        internal void SetDelayOnProcessing(TimeSpan delay)
        {
            partDelay = delay;
        }

        internal async Task StartProcessors(int numberOfProcessors, Channel<PipelineBuffer> readBufferChannel)
        {
            await StartProcessorsWithScalingStrategyAsync(numberOfProcessors, readBufferChannel, ProcessorAsync, cancellationTokenSource);
        }

        private async Task ProcessorAsync(PipelineBuffer buffer, CancellationToken cancellationToken)
        {
            await semaphore.WaitAsync(cancellationToken);

            try
            {
                BufferArg = buffer;
                CancellationTokenArg = cancellationToken;

                Logger.LogInformation("Doing:{Ordinal}: ThrowOnOrdinal {ThrowOnOrdinal}", buffer.Ordinal, throwOnOrdinal);

                if (buffer.Ordinal == throwOnOrdinal)
                {
                    Logger.LogInformation("ProcessorAsync: {Ordinal} throwing", buffer.Ordinal);
                    throw new InvalidOperationException();
                }

                if (partDelay is not null)
                {
                    Logger.LogInformation("ProcessorAsync: {Ordinal} delaying by {Delay}", buffer.Ordinal, partDelay);
                    await Task.Delay(partDelay.Value, cancellationToken);
                }

                ProcessedCount += 1;
            }
            finally
            {
                semaphore.Release();
            }
        }
    }
}
