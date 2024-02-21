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
    protected readonly BlobApiHttpUtils BlobApiHttpUtils = new BlobApiHttpUtils();

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
        //TODO: Right now we are using MaxProcessingTimeScalingStrategy with defaults, but we should be able to use different strategies.        
        var scalingStrategy = new MaxProcessingTimeScalingStrategy();
        partsProducer = new PartsProducer(this, pipelineOptions);
        partsWriter = new PartsWriter(this, pipelineOptions, memoryBuffer, scalingStrategy);
        partsReader = new PartsReader(this, pipelineOptions, memoryBuffer, scalingStrategy);
        processedPartsProcessor = new ProcessedPartsProcessor(this);
    }

    public abstract ValueTask<int> ExecuteWriteAsync(PipelineBuffer buffer, CancellationToken cancellationToken);

    public abstract ValueTask<int> ExecuteReadAsync(PipelineBuffer buffer, CancellationToken cancellationToken);

    public abstract Task<long> GetSourceLengthAsync(string source);

    public abstract Task OnCompletionAsync(long length, Uri? blobUrl, string fileName, string? rootHash);

    public abstract void ConfigurePipelineBuffer(PipelineBuffer buffer);

    protected async Task<long> ExecutePipelineAsync(List<BlobOperationInfo> operations)
    {
        var cancellationSource = new CancellationTokenSource();
        var pipelineTasks = new List<Task>
        {
            partsProducer.StartPartsProducersAsync(operations, ReadBufferChannel, cancellationSource),
            partsReader.StartPartsReaderAsync(ReadBufferChannel,WriteBufferChannel, cancellationSource),
            partsWriter.StartPartsWritersAsync(WriteBufferChannel,ProcessedBufferChannel, cancellationSource)
        };

        var processedPartsProcessorTask = processedPartsProcessor.StartProcessedPartsProcessorAsync(expectedNumberOfFiles: operations.Count, ProcessedBufferChannel, ReadBufferChannel);

        try
        {
            await WhenAllFailFast(pipelineTasks);

            Logger.LogInformation("Pipeline processing completed.");
        }
        catch (Exception e)
        {
            Logger.LogError(e, "Pipeline processing failed.");
            throw;
        }

        Logger.LogInformation("Waiting for processed part processor to complete.");
        var bytesProcessed = await processedPartsProcessorTask;
        Logger.LogInformation("Processed parts completed.");

        return bytesProcessed;
    }

    private static async Task WhenAllFailFast(IEnumerable<Task> tasks)
    {
        var taskList = tasks.ToList();
        while (taskList.Count > 0)
        {
            var completedTask = await Task.WhenAny(taskList);
            if (completedTask.IsFaulted)
            {
                throw completedTask.Exception?.InnerException!;
            }
            if (completedTask.IsCanceled)
            {
                throw new TaskCanceledException("Processing task was canceled.");
            }
            taskList.Remove(completedTask);
        }
    }
}
