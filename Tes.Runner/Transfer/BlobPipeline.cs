// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Tes.Runner.Transfer;

public abstract class BlobPipeline : IBlobPipeline
{
    protected readonly Channel<PipelineBuffer> ReadBufferChannel;
    protected readonly Channel<PipelineBuffer> WriteBufferChannel;
    protected readonly Channel<ProcessedBuffer> ProcessedBufferChannel;
    protected readonly Channel<byte[]> MemoryBufferChannel;
    protected readonly HttpClient HttpClient = new HttpClient() { Timeout = TimeSpan.FromSeconds(300) };
    protected readonly BlobPipelineOptions PipelineOptions;
    protected readonly ILogger Logger = PipelineLoggerFactory.Create<BlobPipeline>();
    private readonly PartsProducer partsProducer;

    public const int MiB = 1024 * 1024;
    public const int DefaultBlockSize = MiB * 100;

    protected BlobPipeline(BlobPipelineOptions pipelineOptions, Channel<byte[]> memoryBuffer)
    {
        ArgumentNullException.ThrowIfNull(pipelineOptions);

        PipelineOptions = pipelineOptions;

        ReadBufferChannel = Channel.CreateBounded<PipelineBuffer>(pipelineOptions.NumberOfBuffers);
        WriteBufferChannel = Channel.CreateBounded<PipelineBuffer>(pipelineOptions.NumberOfBuffers);
        ProcessedBufferChannel = Channel.CreateUnbounded<ProcessedBuffer>();

        MemoryBufferChannel = memoryBuffer;
        partsProducer = new PartsProducer(this,pipelineOptions);
    }
    protected virtual ProcessedBuffer ToProcessedBuffer(PipelineBuffer buffer)
    {
        return new ProcessedBuffer(buffer.FileName, buffer.BlobUrl, buffer.FileSize, buffer.Ordinal,
            buffer.NumberOfParts, buffer.FileHandlerPool, buffer.BlobPartUrl, buffer.Length);
    }

    public abstract ValueTask<int> ExecuteWriteAsync(PipelineBuffer buffer);

    public abstract ValueTask<int> ExecuteReadAsync(PipelineBuffer buffer);

    /// <summary>
    /// This method must return the length in bytes of the source provided. The source is either a URL or path to file.
    /// </summary>
    /// <param name="source">Source path or URL</param>
    /// <returns>Size of the source</returns>
    public abstract Task<long> GetSourceLengthAsync(string source);

    public abstract Task OnCompletionAsync(long length, Uri? blobUrl, string fileName);

    protected async Task<long> ExecutePipelineAsync(List<BlobOperationInfo> operations)
    {
        var pipelineTasks = new List<Task>
        {
            partsProducer.StartPartsProducersAsync(operations, ReadBufferChannel),
            StartReadPipelineAsync(),
            StartWritePipelineAsync()
        };

        var totalBytesProcessed = StartProcessedPipelineAsync(operations.Count);
        
        try
        {
            //await Task.WhenAll(pipelineTasks);
            await WhenAllOrThrowIfOneFailsAsync(pipelineTasks);
        }
        catch (Exception e)
        {
            Logger.LogError(e, "Pipeline processing failed.");
            throw;
        }
        
        return await totalBytesProcessed;
    }


    public abstract void ConfigurePipelineBuffer(PipelineBuffer buffer);

    private async ValueTask<long> StartProcessedPipelineAsync(int numberOfFiles)
    {
        var tasks = new List<Task>();

        ProcessedBuffer? buffer;
        var partsProcessed = new Dictionary<string, int>();
        var allFilesProcessed = false;
        var processedFiles = 0;
        long totalBytes = 0;
        while (!allFilesProcessed && await ProcessedBufferChannel.Reader.WaitToReadAsync())
        {
            while (ProcessedBufferChannel.Reader.TryRead(out buffer))
            {
                totalBytes += buffer.Length;

                if (!partsProcessed.ContainsKey(buffer.FileName))
                {
                    partsProcessed.Add(buffer.FileName, 1);
                    continue;
                }

                var total = ++partsProcessed[buffer.FileName];

                if (total == buffer.NumberOfParts)
                {
                    tasks.Add(OnCompletionAsync(buffer.FileSize, buffer.BlobUrl, buffer.FileName));

                    processedFiles++;

                    await CloseFileHandlerPoolAsync(buffer.FileHandlerPool);

                    allFilesProcessed = processedFiles == numberOfFiles;
                }
            }
        }

        await Task.WhenAll(tasks);

        ReadBufferChannel.Writer.Complete();

        return totalBytes;
    }

    private async ValueTask CloseFileHandlerPoolAsync(Channel<FileStream> bufferFileHandlerPool)
    {
        bufferFileHandlerPool.Writer.Complete();

        await foreach (FileStream fileStream in bufferFileHandlerPool.Reader.ReadAllAsync())
        {
            CloseFileHandler(fileStream);
        }
    }

    private async Task StartWritePipelineAsync()
    {
        List<Task> tasks = new List<Task>();

        Logger.LogDebug($"Starting Write Pipeline with {PipelineOptions.NumberOfWriters} writers");

        for (int i = 0; i < PipelineOptions.NumberOfWriters; i++)
        {
            tasks.Add(Task.Run(async () =>
            {
                PipelineBuffer? buffer;

                while (await WriteBufferChannel.Reader.WaitToReadAsync())
                {
                    while (WriteBufferChannel.Reader.TryRead(out buffer))
                    {
                        try
                        {
                            Logger.LogDebug($"Executing write operation fileName:{buffer.FileName} ordinal:{buffer.Ordinal} numberOfParts:{buffer.NumberOfParts}");

                            await ExecuteWriteAsync(buffer);

                            await ProcessedBufferChannel.Writer.WriteAsync(ToProcessedBuffer(buffer));

                            await MemoryBufferChannel.Writer.WriteAsync(buffer.Data);
                        }
                        catch (Exception e)
                        {
                            Logger.LogError(e, "Failed to execute write operation");
                            throw;
                        }
                    }
                }
            }));
        }

        await Task.WhenAll(tasks);

        ProcessedBufferChannel.Writer.Complete();
    }
    private async Task StartReadPipelineAsync()
    {
        var tasks = new List<Task>();

        Logger.LogInformation($"Starting Read Pipeline with {PipelineOptions.NumberOfReaders} readers");

        for (int i = 0; i < PipelineOptions.NumberOfReaders; i++)
        {
            tasks.Add(Task.Run(async () =>
                {
                    PipelineBuffer? buffer;

                    while (await ReadBufferChannel.Reader.WaitToReadAsync())
                    {

                        while (ReadBufferChannel.Reader.TryRead(out buffer))
                        {
                            try
                            {

                                buffer.Data = await MemoryBufferChannel.Reader.ReadAsync();

                                Logger.LogDebug($"Executing read operation fileName:{buffer.FileName} ordinal:{buffer.Ordinal} numberOfParts:{buffer.NumberOfParts}");

                                await ExecuteReadAsync(buffer);

                                await WriteBufferChannel.Writer.WriteAsync(buffer);
                            }
                            catch (Exception e)
                            {
                                Logger.LogError(e, "Failed to execute read operation.");
                                throw;
                            }
                        }
                    }
                }
            ));
        }

        await Task.WhenAll(tasks);

        WriteBufferChannel.Writer.Complete();
    }
    //private async Task StartAndWaitPipelineWorkersAsync(int numberOfWorkers, Channel<PipelineBuffer> pipelineChannel, Func<PipelineBuffer,ValueTask> onReadAsync, Action<Channel<PipelineBuffer>>? onDone)
    //{
    //    var tasks = new List<Task>();

    //    for (var workers = 0; workers < numberOfWorkers; workers++)
    //    {
    //        tasks.Add(Task.Run(async () =>
    //        {
    //            PipelineBuffer? buffer;

    //            while (await pipelineChannel.Reader.WaitToReadAsync())
    //            {
    //                while (pipelineChannel.Reader.TryRead(out buffer))
    //                {
    //                    try
    //                    {
    //                        await onReadAsync(buffer);
    //                    }
    //                    catch (Exception e)
    //                    {
    //                        Logger.LogError(e, "Failed to process pipeline buffer");
    //                        throw;
    //                    }

    //                }
    //            }
    //        }));
    //    }

    //    await Task.WhenAll(tasks);

    //    onDone?.Invoke(pipelineChannel);
    //}

    private void CloseFileHandler(FileStream? fileStream)
    {
        if (fileStream is not null && !fileStream.SafeFileHandle.IsClosed)
        {
            fileStream.Close();
        }
    }

    private async Task WhenAllOrThrowIfOneFailsAsync(List<Task> tasks)
    {
        var tasksPending = tasks.ToList();
        while (tasksPending.Any())
        {
            var completedTask = await Task.WhenAny(tasksPending);

            tasksPending.Remove(completedTask);

            if (completedTask.IsFaulted)
            {
                throw new Exception("At least one of the tasks has failed.", completedTask.Exception);
            }
        }
    }
}
