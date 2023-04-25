using System.Collections.Concurrent;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Moq;
using Tes.Runner.Transfer;

namespace Tes.Runner.Test
{
    [TestClass]
    [TestCategory("unit")]
    public class BlobPipelineTest
    {
        private BlobPipelineTestImpl pipeline;
        private BlobPipelineOptions options;
        private readonly int blockSize = BlobPipeline.MiB;
        private readonly long sourceSize = BlobPipeline.MiB * 10;
        private string tempFile;
        private Channel<byte[]> memoryBuffer;
        
        [TestInitialize]
        public async Task SetUp()
        {
            tempFile = $"{Guid.NewGuid()}.tmp";
            await using var fs = File.Create(tempFile);
            fs.Close();

             memoryBuffer = await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(5, blockSize);

            options = new BlobPipelineOptions(blockSize, 10, 10, 10);
            pipeline = new BlobPipelineTestImpl(options, memoryBuffer, sourceSize);

        }

        [TestCleanup]
        public void CleanUp()
        {
            if (File.Exists(tempFile))
            {
                File.Delete(tempFile);
            }
        }


        [TestMethod]
        public async Task ExecuteAsync_SingleOperation_CallsReaderWriterAndCompleteMethods_CorrectNumberOfTimes()
        {
            var blobOp = new BlobOperationInfo(new Uri("https://foo.bar/con/blob"), tempFile, tempFile, true);

            await pipeline.ExecuteAsync(new List<BlobOperationInfo>() {blobOp});

            //the number of calls should be size of the file divided by the number blocks
            var expectedNumberOfCalls = (sourceSize / blockSize);

            AssertReaderWriterAndCompleteMethodsAreCalled(pipeline, expectedNumberOfCalls);
        }

        [TestMethod]
        public async Task ExecuteAsync_TwoOperations_CallsReaderWriterAndCompleteMethods_CorrectNumberOfTimes()
        {
            var pipeline = new BlobPipelineTestImpl(options, memoryBuffer, sourceSize);
            
            var blobOps = new List<BlobOperationInfo>()
            {
                new BlobOperationInfo(new Uri("https://foo.bar/con/blob"), tempFile, tempFile, true),
                new BlobOperationInfo(new Uri("https://foo.bar/con/blob"), tempFile, tempFile, true)
            };
            await pipeline.ExecuteAsync(blobOps);

            //the number of calls should be size of the file divided by the number blocks, times the number of files
            var expectedNumberOfCalls = (sourceSize /blockSize) * blobOps.Count;

            AssertReaderWriterAndCompleteMethodsAreCalled(pipeline, expectedNumberOfCalls);
        }

        private void AssertReaderWriterAndCompleteMethodsAreCalled(BlobPipelineTestImpl pipeline, long numberOfTimes)
        {
            List<MethodCall> executeWriteInfo = pipeline.MethodCalls["ExecuteWriteAsync"];
            Assert.IsNotNull(executeWriteInfo);
            Assert.AreEqual(numberOfTimes, executeWriteInfo.Count);

            var executeReadInfo = pipeline.MethodCalls["ExecuteReadAsync"];
            Assert.IsNotNull(executeReadInfo);
            Assert.AreEqual(numberOfTimes, executeWriteInfo.Count);

            var onCompletionInfo = pipeline.MethodCalls["OnCompletionAsync"];
            Assert.IsNotNull(onCompletionInfo);
            //complete must always be one
            Assert.AreEqual(1, onCompletionInfo.Count);
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
            

        public BlobPipelineTestImpl(BlobPipelineOptions pipelineOptions, Channel<byte[]> memoryBuffer, long sourceLength) : base(pipelineOptions, memoryBuffer)
        {
            this.sourceLength = sourceLength;
        }

        public ConcurrentDictionary<string, List<MethodCall>> MethodCalls => methodCalls;

        protected override ValueTask<int> ExecuteWriteAsync(PipelineBuffer buffer)
        {
            AddMethodCall(nameof(ExecuteWriteAsync), buffer);
            return ValueTask.FromResult(buffer.Length);
        }

        protected override ValueTask<int> ExecuteReadAsync(PipelineBuffer buffer)
        {
            AddMethodCall(nameof(ExecuteReadAsync), buffer);
            return ValueTask.FromResult(buffer.Length);
        }

        protected override Task<long> GetSourceLength(string source)
        {
            AddMethodCall(nameof(GetSourceLength), source);
            return Task.FromResult(sourceLength);
        }

        protected override Task OnCompletionAsync(long length, Uri? blobUrl, string fileName)
        {
            AddMethodCall(nameof(OnCompletionAsync), length, blobUrl, fileName);
            return Task.CompletedTask;
        }

        public async Task<long> ExecuteAsync(List<BlobOperationInfo> blobOperations)
        {
            return await ExecutePipelineAsync(blobOperations);
        }

        private void AddMethodCall(string methodName, params Object[] args)
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

    }

    record MethodCall(string MethodName, int InvocationTime, List<object> Parameters);
}
