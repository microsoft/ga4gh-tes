using System.Collections.Concurrent;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Moq;
using Tes.Runner.Transfer;

namespace Tes.Runner.Test
{
    [TestClass]
    [TestCategory("unit")]
    public class BlobPipelineTests
    {
        private BlobPipelineTestImpl pipeline;
        private BlobPipelineOptions options;
        private readonly int blockSize = BlobPipeline.MiB;
        private readonly long sourceSize = BlobPipeline.MiB * 10;
        private string tempFile1;
        private string tempFile2;
        private Channel<byte[]> memoryBuffer;
        private readonly RunnerTestUtils runnerTestUtils = new RunnerTestUtils();

        [TestInitialize]
        public async Task SetUp()
        {
            tempFile1 = await RunnerTestUtils.CreateTempFileAsync();
            tempFile2 = await RunnerTestUtils.CreateTempFileAsync();

            memoryBuffer = await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(5, blockSize);

            options = new BlobPipelineOptions(blockSize, 10, 10, 10);
            pipeline = new BlobPipelineTestImpl(options, memoryBuffer, sourceSize);

        }

        [TestCleanup]
        public void CleanUp()
        {
            RunnerTestUtils.DeleteFileIfExists(tempFile1);
            RunnerTestUtils.DeleteFileIfExists(tempFile2);
        }


        [TestMethod]
        public async Task ExecuteAsync_SingleOperation_CallsReaderWriterAndCompleteMethods_CorrectNumberOfTimes()
        {
            var blobOp = new BlobOperationInfo(new Uri("https://foo.bar/con/blob"), tempFile1, tempFile1, true);

            await pipeline.ExecuteAsync(new List<BlobOperationInfo>() {blobOp});

            //the number of calls should be size of the file divided by the number blocks
            var expectedNumberOfCalls = (sourceSize / blockSize);

            AssertReaderWriterAndCompleteMethodsAreCalled(pipeline, expectedNumberOfCalls, 1);
        }

        [TestMethod]
        public async Task ExecuteAsync_TwoOperations_CallsReaderWriterAndCompleteMethods_CorrectNumberOfTimes()
        {
            var pipeline = new BlobPipelineTestImpl(options, memoryBuffer, sourceSize);
            
            var blobOps = new List<BlobOperationInfo>()
            {
                new BlobOperationInfo(new Uri("https://foo.bar/con/blob1"), tempFile1, tempFile1, true),
                new BlobOperationInfo(new Uri("https://foo.bar/con/blob2"), tempFile2, tempFile2, true)
            };
            await pipeline.ExecuteAsync(blobOps);

            //the number of calls should be size of the file divided by the number blocks, times the number of files
            var expectedNumberOfCalls = (sourceSize /blockSize) * blobOps.Count;

            AssertReaderWriterAndCompleteMethodsAreCalled(pipeline, expectedNumberOfCalls, 2);
        }

        private void AssertReaderWriterAndCompleteMethodsAreCalled(BlobPipelineTestImpl pipeline, long numberOfWriterReaderCalls, int numberOfCompleteCalls)
        {
            List<MethodCall> executeWriteInfo = pipeline.MethodCalls["ExecuteWriteAsync"];
            Assert.IsNotNull(executeWriteInfo);
            Assert.AreEqual(numberOfWriterReaderCalls, executeWriteInfo.Count);

            var executeReadInfo = pipeline.MethodCalls["ExecuteReadAsync"];
            Assert.IsNotNull(executeReadInfo);
            Assert.AreEqual(numberOfWriterReaderCalls, executeWriteInfo.Count);

            var onCompletionInfo = pipeline.MethodCalls["OnCompletionAsync"];
            Assert.IsNotNull(onCompletionInfo);
            //complete must always be one
            Assert.AreEqual(numberOfCompleteCalls, onCompletionInfo.Count);
        }
    }

    /// <summary>
    /// This is a test implementation of BlobPipeline.
    /// Since there is no way to mock the base class, we have to create a test implementation and capture the execution of methods directly.
    /// </summary>
    class BlobPipelineTestImpl : BlobPipeline
    {
        private readonly ConcurrentDictionary<string, List<MethodCall>> methodCalls =
            new ConcurrentDictionary<string, List<MethodCall>>();

        private readonly long sourceLength;
            
        private readonly SemaphoreSlim semaphore = new SemaphoreSlim(1);

        public BlobPipelineTestImpl(BlobPipelineOptions pipelineOptions, Channel<byte[]> memoryBuffer, long sourceLength) : base(pipelineOptions, memoryBuffer)
        {
            this.sourceLength = sourceLength;
        }

        public ConcurrentDictionary<string, List<MethodCall>> MethodCalls => methodCalls;

        public override ValueTask<int> ExecuteWriteAsync(PipelineBuffer buffer)
        {
            AddMethodCall(nameof(ExecuteWriteAsync), buffer);
            return ValueTask.FromResult(buffer.Length);
        }

        public override ValueTask<int> ExecuteReadAsync(PipelineBuffer buffer)
        {
            AddMethodCall(nameof(ExecuteReadAsync), buffer);
            return ValueTask.FromResult(buffer.Length);
        }

        public override Task<long> GetSourceLengthAsync(string source)
        {
            AddMethodCall(nameof(GetSourceLengthAsync), source);
            return Task.FromResult(sourceLength);
        }

        public override Task OnCompletionAsync(long length, Uri? blobUrl, string fileName)
        {
            AddMethodCall(nameof(OnCompletionAsync), length, blobUrl, fileName);
            return Task.CompletedTask;
        }

        public override void ConfigurePipelineBuffer(PipelineBuffer buffer)
        {
            AddMethodCall(nameof(ConfigurePipelineBuffer), buffer);
        }

        public async Task<long> ExecuteAsync(List<BlobOperationInfo> blobOperations)
        {
           var data = await ExecutePipelineAsync(blobOperations);

           return data;
        }

        private void AddMethodCall(string methodName, params Object[] args)
        {
            //the add/update factories are not thread safe, hence the semaphore here...
            semaphore.Wait();

            try
            {
                methodCalls.AddOrUpdate(methodName,
                    (key) => new List<MethodCall>() { new MethodCall(key, 1, args.ToList()) },
                    (key, value) =>
                    {
                        value.Add(new MethodCall(methodName, value.Count + 1,
                            args.ToList()));
                        return value;
                    });

            }
            finally
            {
                semaphore.Release();
            }
        }

    }

    record MethodCall(string MethodName, int InvocationTime, List<object> Parameters);
}
