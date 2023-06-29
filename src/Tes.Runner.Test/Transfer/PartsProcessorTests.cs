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
        private readonly Random random = new Random();

        [TestInitialize]
        public void SetUp()
        {
            readChannel = Channel.CreateBounded<PipelineBuffer>(RunnerTestUtils.PipelineBufferCapacity);
            partsProcessor = new TestPartsProcessor(new Mock<IBlobPipeline>().Object, new BlobPipelineOptions(),
                               Channel.CreateBounded<byte[]>(RunnerTestUtils.MemBuffersCapacity));
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

            var tasks = partsProcessor.StartProcessors(numOfProcessors, readChannel);
            Exception? exception = null;
            try
            {
                await Task.WhenAll(tasks);
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
            var tasks = partsProcessor.StartProcessors(numOfProcessors, readChannel);

            await Task.WhenAll(tasks);

            Assert.AreEqual(expectedCount, partsProcessor.ProcessedCount);
            Assert.IsFalse(partsProcessor.CancellationTokenArg.IsCancellationRequested);
        }

        private async Task PrepareReaderChannelAsync()
        {
            await RunnerTestUtils.PreparePipelineChannelAsync(blockSizeBytes, fileSize, fileName, blobUri, readChannel);
        }
    }

    internal class TestPartsProcessor : PartsProcessor
    {
        internal PipelineBuffer BufferArg { get; private set; } = null!;
        internal CancellationToken CancellationTokenArg { get; private set; }
        private readonly ILogger logger = PipelineLoggerFactory.Create<TestPartsProcessor>();
        private readonly SemaphoreSlim semaphore = new SemaphoreSlim(1, 1);
        internal int ProcessedCount { get; private set; }
        private int? throwOnOrdinal;

        internal TestPartsProcessor(IBlobPipeline blobPipeline, BlobPipelineOptions blobPipelineOptions,
            Channel<byte[]> memoryBufferChannel) : base(blobPipeline, blobPipelineOptions, memoryBufferChannel)
        {
        }

        internal void SetThrowOnOrdinal(int ordinal)
        {
            throwOnOrdinal = ordinal;
        }

        internal List<Task> StartProcessors(int numberOfProcessors, Channel<PipelineBuffer> readBufferChannel)
        {
            return StartProcessors(numberOfProcessors, readBufferChannel, ProcessorAsync);
        }

        private async Task ProcessorAsync(PipelineBuffer buffer, CancellationToken cancellationToken)
        {
            await semaphore.WaitAsync();

            try
            {
                BufferArg = buffer;
                CancellationTokenArg = cancellationToken;

                logger.LogInformation($"Doing:{buffer.Ordinal}: ThrowOnOrdinal {throwOnOrdinal}");

                if (buffer.Ordinal == throwOnOrdinal)
                {
                    logger.LogInformation($"ProcessorAsync: {buffer.Ordinal} throwing");
                    throw new InvalidOperationException();
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
