// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Tes.Runner.Transfer;

public abstract class BlobPipeline
{
    protected readonly Channel<PipelineBuffer> ReadBufferChannel;
    protected readonly Channel<PipelineBuffer> WriteBufferChannel;
    protected readonly Channel<ProcessedBuffer> ProcessedBufferChannel;
    protected readonly Channel<byte[]> MemoryBufferChannel;
    protected readonly HttpClient HttpClient = new HttpClient() { Timeout = TimeSpan.FromSeconds(300) };
    protected readonly BlobPipelineOptions PipelineOptions;
    protected readonly ILogger Logger = PipelineLoggerFactory.Create<BlobPipeline>();

    public const int MiB = 1024 * 1024;
    public const int DefaultBlockSize = MiB * 100;
    private const int MaxNumberBlobParts = 50000;

    protected BlobPipeline(BlobPipelineOptions pipelineOptions, Channel<byte[]> memoryBuffer)
    {
        ArgumentNullException.ThrowIfNull(pipelineOptions);

        PipelineOptions = pipelineOptions;

        ReadBufferChannel = Channel.CreateBounded<PipelineBuffer>(pipelineOptions.NumberOfBuffers);
        WriteBufferChannel = Channel.CreateBounded<PipelineBuffer>(pipelineOptions.NumberOfBuffers);
        ProcessedBufferChannel = Channel.CreateUnbounded<ProcessedBuffer>();

        MemoryBufferChannel = memoryBuffer;
    }
    protected virtual ProcessedBuffer ToProcessedBuffer(PipelineBuffer buffer)
    {
        return new ProcessedBuffer(buffer.FileName, buffer.BlobUrl, buffer.FileSize, buffer.Ordinal,
            buffer.NumberOfParts, buffer.FileHandlerPool, buffer.BlobPartUrl, buffer.Length);
    }

    protected abstract ValueTask<int> ExecuteWriteAsync(PipelineBuffer buffer);

    protected abstract ValueTask<int> ExecuteReadAsync(PipelineBuffer buffer);

    protected abstract Task<long> GetSourceLength(string source);

    protected abstract Task OnCompletionAsync(long length, Uri? blobUrl, string fileName);

    protected int GetNumberOfParts(long length)
    {
        int numberOfParts = (int)Math.Ceiling((double)(length) / PipelineOptions.BlockSize);

        if (numberOfParts > BlobUploader.MaxNumberBlobParts)
        {
            throw new Exception(
                $"The number of blocks exceeds the maximum allowed by the service of {BlobUploader.MaxNumberBlobParts}. Try increasing the block size. Current block size: {PipelineOptions.BlockSize}");
        }

        return numberOfParts;
    }

    protected async Task<long> ExecutePipelineAsync(List<BlobOperationInfo> operations)
    {
        var pipelineTasks = new List<Task>();

        pipelineTasks.Add(StartPartsProducersAsync(operations).AsTask());
        pipelineTasks.Add(StartReadPipelineAsync().AsTask());
        pipelineTasks.Add(StartWritePipelineAsync().AsTask());

        var totalBytesProcessed = StartProcessedPipelineAsync(operations.Count);

        Logger.LogInformation(
            @$"
Started BlobOperation: 
    Number of Operations: {operations.Count}
    Number of Writers: {PipelineOptions.NumberOfWriters}
    Number of Readers: {PipelineOptions.NumberOfReaders}
    BlockSize: {PipelineOptions.BlockSize}
            ");

        await Task.WhenAll(pipelineTasks);

        return await totalBytesProcessed;
    }

    private PipelineBuffer GetNewPipelinePartBuffer(Uri blobUrl, string fileName, Channel<FileStream> fileHandlerPool,
        long fileSize, int partOrdinal,
        int numberOfParts)
    {
        var buffer = new PipelineBuffer()
        {
            BlobUrl = blobUrl,
            Offset = (long)partOrdinal * PipelineOptions.BlockSize,
            Length = PipelineOptions.BlockSize,
            FileName = fileName,
            Ordinal = partOrdinal,
            NumberOfParts = numberOfParts,
            FileHandlerPool = fileHandlerPool,
            FileSize = fileSize,
        };

        if (partOrdinal == numberOfParts - 1)
        {
            buffer.Length = (int)(fileSize - buffer.Offset);
        }

        ConfigurePipelineBuffer(buffer);

        return buffer;
    }

    protected virtual void ConfigurePipelineBuffer(PipelineBuffer buffer)
    {
        //no configuration..
    }

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

    private async ValueTask StartPartsProducersAsync(List<BlobOperationInfo> blobOperations)
    {
        var partsProducerTasks = new List<Task>();
        foreach (var operation in blobOperations)
        {
            partsProducerTasks.Add(Task.Run(async () =>
            {
                var length = await GetSourceLength(operation.SourceLocationForLength);

                return StartPartsProducer(operation.Url, operation.FileName, length, operation.ReadOnlyFileHandler).AsTask();
            }));
        }

        await Task.WhenAll(partsProducerTasks);
    }

    private async ValueTask StartPartsProducer(Uri url, string fileName, long length, bool readOnlyFileHandle)
    {
        int numberOfParts = GetNumberOfParts(length);

        PipelineBuffer? buffer;

        var fileHandlerPool = await GetNewFileHandlerPoolAsync(fileName, readOnlyFileHandle);

        for (int i = 0; i < numberOfParts; i++)
        {
            buffer = GetNewPipelinePartBuffer(url, fileName, fileHandlerPool, length, i, numberOfParts);

            await ReadBufferChannel.Writer.WriteAsync(buffer);
        }

    }

    private async ValueTask<Channel<FileStream>> GetNewFileHandlerPoolAsync(string fileName, bool readOnly)
    {
        var pool = Channel.CreateBounded<FileStream>(PipelineOptions.BufferCapacity);

        for (int f = 0; f < PipelineOptions.BufferCapacity; f++)
        {
            if (readOnly)
            {
                await pool.Writer.WriteAsync(File.Open(fileName, FileMode.Open, FileAccess.Read, FileShare.Read));
                continue;
            }
            var directory = Path.GetDirectoryName(fileName);

            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            await pool.Writer.WriteAsync(File.Open(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite));
        }

        return pool;
    }

    private async ValueTask StartWritePipelineAsync()
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
    private async ValueTask StartReadPipelineAsync()
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
    private async ValueTask StartAndWaitPipelineWorkersAsync(int numberOfWorkers, Channel<PipelineBuffer> pipelineChannel, Func<PipelineBuffer,ValueTask> onReadAsync, Action<Channel<PipelineBuffer>>? onDone)
    {
        var tasks = new List<Task>();

        for (var workers = 0; workers < numberOfWorkers; workers++)
        {
            tasks.Add(Task.Run(async () =>
            {
                PipelineBuffer? buffer;

                while (await pipelineChannel.Reader.WaitToReadAsync())
                {
                    while (pipelineChannel.Reader.TryRead(out buffer))
                    {
                        try
                        {
                            await onReadAsync(buffer);
                        }
                        catch (Exception e)
                        {
                            Logger.LogError(e, "Failed to process pipeline buffer");
                            throw;
                        }

                    }
                }
            }));
        }

        await Task.WhenAll(tasks);

        onDone?.Invoke(pipelineChannel);
    }

    private void CloseFileHandler(FileStream? fileStream)
    {
        if (fileStream is not null && !fileStream.SafeFileHandle.IsClosed)
        {
            fileStream.Close();
        }
    }
}
