// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Tes.Runner.Transfer;
/// <summary>
/// Base class for all blob operation pipelines to transfer data from and to blob storage.
/// </summary>
public abstract class BlobOperationPipeline : IBlobPipeline
{
    protected readonly Channel<PipelineBuffer> ReadBufferChannel;
    protected readonly Channel<PipelineBuffer> WriteBufferChannel;
    protected readonly Channel<ProcessedBuffer> ProcessedBufferChannel;
    protected readonly Channel<byte[]> MemoryBufferChannel;
    protected readonly BlobPipelineOptions PipelineOptions;
    protected readonly ILogger Logger = PipelineLoggerFactory.Create<BlobOperationPipeline>();

    private readonly PartsProducer partsProducer;
    private readonly PartsWriter partsWriter;
    private readonly PartsReader partsReader;

    private readonly ProcessedPartsProcessor processedPartsProcessor;

    protected BlobOperationPipeline(BlobPipelineOptions pipelineOptions, Channel<byte[]> memoryBuffer)
    {
        ArgumentNullException.ThrowIfNull(pipelineOptions);

        PipelineOptions = pipelineOptions;

        ReadBufferChannel = Channel.CreateBounded<PipelineBuffer>(pipelineOptions.ReadWriteBuffersCapacity);
        WriteBufferChannel = Channel.CreateBounded<PipelineBuffer>(pipelineOptions.ReadWriteBuffersCapacity);
        ProcessedBufferChannel = Channel.CreateUnbounded<ProcessedBuffer>();

        MemoryBufferChannel = memoryBuffer;
        partsProducer = new PartsProducer(this, pipelineOptions);
        partsWriter = new PartsWriter(this, pipelineOptions, memoryBuffer);
        partsReader = new PartsReader(this, pipelineOptions, memoryBuffer);
        processedPartsProcessor = new ProcessedPartsProcessor(this);
    }

    public abstract ValueTask<int> ExecuteWriteAsync(PipelineBuffer buffer);

    public abstract ValueTask<int> ExecuteReadAsync(PipelineBuffer buffer);

    public abstract Task<long> GetSourceLengthAsync(string source);

    public abstract Task OnCompletionAsync(long length, Uri? blobUrl, string fileName,
        Md5Processor? bufferBufferMd5Processor);

    public abstract void ConfigurePipelineBuffer(PipelineBuffer buffer);

    protected async Task<long> ExecutePipelineAsync(List<BlobOperationInfo> operations)
    {
        var pipelineTasks = new List<Task>
        {
            partsProducer.StartPartsProducersAsync(operations, ReadBufferChannel),
            partsReader.StartPartsReaderAsync(ReadBufferChannel,WriteBufferChannel),
            partsWriter.StartPartsWritersAsync(WriteBufferChannel,ProcessedBufferChannel)
        };

        var processedPartsProcessorTask = processedPartsProcessor.StartProcessedPartsProcessorAsync(expectedNumberOfFiles: operations.Count, ProcessedBufferChannel, ReadBufferChannel);

        try
        {
            await PartsProcessor.WhenAllOrThrowIfOneFailsAsync(pipelineTasks);
        }
        catch (Exception e)
        {
            Logger.LogError(e, "Pipeline processing failed.");
            throw;
        }

        return await processedPartsProcessorTask;
    }
}
