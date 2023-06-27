// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Tes.Runner.Transfer;

namespace Tes.Runner.Test
{
    [TestClass]
    [TestCategory("Unit")]
    public class BlobPipelineTests
    {
        private BlobOperationPipelineTestImpl operationPipeline = null!;
        private BlobPipelineOptions options = null!;
        private readonly int blockSize = BlobSizeUtils.MiB;
        private readonly long sourceSize = BlobSizeUtils.MiB * 10;
        private string tempFile1 = null!;
        private string tempFile2 = null!;
        private Channel<byte[]> memoryBuffer = null!;

        [TestInitialize]
        public async Task SetUp()
        {
            tempFile1 = await RunnerTestUtils.CreateTempFileAsync();
            tempFile2 = await RunnerTestUtils.CreateTempFileAsync();

            memoryBuffer = await MemoryBufferPoolFactory.CreateMemoryBufferPoolAsync(5, blockSize);

            options = new BlobPipelineOptions(blockSize, 10, 10, 10);
            operationPipeline = new BlobOperationPipelineTestImpl(options, memoryBuffer, sourceSize);
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

            await operationPipeline.ExecuteAsync(new List<BlobOperationInfo>() { blobOp });

            //the number of calls should be size of the file divided by the number blocks
            var expectedNumberOfCalls = (sourceSize / blockSize);

            AssertReaderWriterAndCompleteMethodsAreCalled(operationPipeline, expectedNumberOfCalls, 1);
        }

        [TestMethod]
        public async Task ExecuteAsync_TwoOperations_CallsReaderWriterAndCompleteMethods_CorrectNumberOfTimes()
        {
            var pipeline = new BlobOperationPipelineTestImpl(options, memoryBuffer, sourceSize);

            var blobOps = new List<BlobOperationInfo>()
            {
                new BlobOperationInfo(new Uri("https://foo.bar/con/blob1"), tempFile1, tempFile1, true),
                new BlobOperationInfo(new Uri("https://foo.bar/con/blob2"), tempFile2, tempFile2, true)
            };
            await pipeline.ExecuteAsync(blobOps);

            //the number of calls should be size of the file divided by the number blocks, times the number of files
            var expectedNumberOfCalls = (sourceSize / blockSize) * blobOps.Count;

            AssertReaderWriterAndCompleteMethodsAreCalled(pipeline, expectedNumberOfCalls, 2);
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException))]
        public async Task ExecuteAsync_ThrowsOnRead_ExecutesThrows()
        {

            var pipeline = new BlobOperationPipelineTestImpl(options, memoryBuffer, sourceSize);

            //throw on when processing the 5th block
            pipeline.ThrowOnExecuteRead<InvalidOperationException>((buffer, token) => buffer.Ordinal == 5);

            var blobOps = new List<BlobOperationInfo>()
            {
                new BlobOperationInfo(new Uri("https://foo.bar/con/blob1"), tempFile1, tempFile1, true),
                new BlobOperationInfo(new Uri("https://foo.bar/con/blob2"), tempFile2, tempFile2, true)
            };

            await pipeline.ExecuteAsync(blobOps);
        }

        private static void AssertReaderWriterAndCompleteMethodsAreCalled(BlobOperationPipelineTestImpl operationPipeline, long numberOfWriterReaderCalls, int numberOfCompleteCalls)
        {
            var executeWriteInfo = operationPipeline.MethodCalls["ExecuteWriteAsync"];
            Assert.IsNotNull(executeWriteInfo);
            Assert.AreEqual(numberOfWriterReaderCalls, executeWriteInfo.Count);

            var executeReadInfo = operationPipeline.MethodCalls["ExecuteReadAsync"];
            Assert.IsNotNull(executeReadInfo);
            Assert.AreEqual(numberOfWriterReaderCalls, executeWriteInfo.Count);

            var onCompletionInfo = operationPipeline.MethodCalls["OnCompletionAsync"];
            Assert.IsNotNull(onCompletionInfo);
            //complete must always be one
            Assert.AreEqual(numberOfCompleteCalls, onCompletionInfo.Count);
        }
    }

    /// <summary>
    /// This is a test implementation of BlobPipeline.
    /// Since there is no way to mock the base class, we have to create a test implementation and capture the execution of methods directly.
    /// </summary>
    class BlobOperationPipelineTestImpl : BlobOperationPipeline
    {
        private readonly ConcurrentDictionary<string, List<MethodCall>> methodCalls = new();

        private readonly long sourceLength;

        private readonly SemaphoreSlim semaphore = new(1);
        private Func<PipelineBuffer, CancellationToken, bool>? throwOnExecuteWrite = null!;
        private Exception? exceptionOnExecuteWrite = null!;
        private Func<PipelineBuffer, CancellationToken, bool>? throwOnExecuteRead = null!;
        private Exception? exceptionOnExecuteRead = null!;
        public ConcurrentDictionary<string, List<MethodCall>> MethodCalls => methodCalls;

        public BlobOperationPipelineTestImpl(BlobPipelineOptions pipelineOptions, Channel<byte[]> memoryBuffer, long sourceLength) : base(pipelineOptions, memoryBuffer)
        {
            this.sourceLength = sourceLength;
        }

        public void ThrowOnExecuteWrite<T>(Func<PipelineBuffer, CancellationToken, bool> predicate)
            where T : Exception, new()
        {
            exceptionOnExecuteWrite = new T();
            throwOnExecuteWrite = predicate;
        }
        public void ThrowOnExecuteRead<T>(Func<PipelineBuffer, CancellationToken, bool> predicate)
            where T : Exception, new()
        {
            exceptionOnExecuteRead = new T();
            throwOnExecuteRead = predicate;
        }

        public override ValueTask<int> ExecuteWriteAsync(PipelineBuffer buffer, CancellationToken cancellationToken)
        {
            AddMethodCall(nameof(ExecuteWriteAsync), buffer, cancellationToken);

            if (throwOnExecuteWrite != null && throwOnExecuteWrite(buffer, cancellationToken))
            {
                throw exceptionOnExecuteWrite!;
            }

            return ValueTask.FromResult(buffer.Length);
        }

        public override ValueTask<int> ExecuteReadAsync(PipelineBuffer buffer, CancellationToken cancellationToken)
        {
            AddMethodCall(nameof(ExecuteReadAsync), buffer, cancellationToken);

            if (throwOnExecuteRead != null && throwOnExecuteRead(buffer, cancellationToken))
            {
                throw exceptionOnExecuteRead!;
            }

            return ValueTask.FromResult(buffer.Length);
        }

        public override Task<long> GetSourceLengthAsync(string source)
        {
            AddMethodCall(nameof(GetSourceLengthAsync), source);
            return Task.FromResult(sourceLength);
        }

        public override Task OnCompletionAsync(long length, Uri? blobUrl, string fileName, string? rootHash)
        {
            Debug.Assert(blobUrl != null, nameof(blobUrl) + " != null");
            AddMethodCall(nameof(OnCompletionAsync), length, blobUrl, fileName, rootHash!);
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
                Logger.LogInformation($"Adding method call {methodName} with args {args}");
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
