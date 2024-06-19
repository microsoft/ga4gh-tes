// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Tes.Runner.Transfer;
/// <summary>
/// Base class for the parts processorAsync.
/// </summary>
public abstract class PartsProcessor
{
    protected readonly IBlobPipeline BlobPipeline;
    protected readonly Channel<byte[]> MemoryBufferChannel;
    protected readonly BlobPipelineOptions BlobPipelineOptions;
    protected readonly ILogger Logger;
    private readonly IScalingStrategy scalingStrategy;

    private TimeSpan currentMaxPartProcessingTime;

    private readonly SemaphoreSlim semaphore = new(1);

    protected PartsProcessor(IBlobPipeline blobPipeline, BlobPipelineOptions blobPipelineOptions, Channel<byte[]> memoryBufferChannel, IScalingStrategy scalingStrategy, ILogger logger)
    {
        ValidateArguments(blobPipeline, blobPipelineOptions, memoryBufferChannel);

        BlobPipeline = blobPipeline;
        BlobPipelineOptions = blobPipelineOptions;
        MemoryBufferChannel = memoryBufferChannel;
        this.scalingStrategy = scalingStrategy;
        this.Logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    private static void ValidateArguments(IBlobPipeline blobPipeline, BlobPipelineOptions blobPipelineOptions,
        Channel<byte[]> memoryBufferChannel)
    {
        ArgumentNullException.ThrowIfNull(blobPipelineOptions);
        ArgumentNullException.ThrowIfNull(blobPipeline);
        ArgumentNullException.ThrowIfNull(memoryBufferChannel);

        if (blobPipelineOptions.NumberOfWriters < 1)
        {
            throw new ArgumentException("The number of writers must be greater than 0.");
        }

        if (blobPipelineOptions.NumberOfReaders < 1)
        {
            throw new ArgumentException("The number of readers must be greater than 0.");
        }

        if (blobPipelineOptions.ReadWriteBuffersCapacity < 1)
        {
            throw new ArgumentException("The buffer capacity must be greater than 0.");
        }

        if (blobPipelineOptions.MemoryBufferCapacity < 1)
        {
            throw new ArgumentException("The memory buffer capacity must be greater than 0.");
        }
    }

    protected Task StartProcessorsWithScalingStrategyAsync(int numberOfProcessors, Channel<PipelineBuffer> readFromChannel, Func<PipelineBuffer, CancellationToken, Task> processorAsync, CancellationTokenSource cancellationSource)
    {
        return Task.Run(async () =>
        {
            var tasks = new List<Task>();

            for (var p = 0; p < numberOfProcessors; p++)
            {
                try
                {
                    await semaphore.WaitAsync(cancellationSource.Token);

                    if (!scalingStrategy.IsScalingAllowed(p, currentMaxPartProcessingTime))
                    {
                        Logger.LogInformation("The maximum number of tasks for the transfer operation has been set. Max part processing time is: {currentMaxPartProcessingTimeInMs} ms. Processing tasks count: {processorCount}.", currentMaxPartProcessingTime, p);
                        break;
                    }
                }
                finally
                {
                    semaphore.Release();
                }

                if (readFromChannel.Reader.Completion.IsCompleted)
                {
                    Logger.LogInformation("The readFromChannel is completed, no need to add more processing tasks. Processing tasks count: {processorCount}.", p);
                    break;
                }

                var delay = scalingStrategy.GetScalingDelay(p);

                Logger.LogInformation("Increasing the number of processing tasks to {processorCount}", p + 1);

                tasks.Add(StartProcessorTaskAsync(readFromChannel, processorAsync, cancellationSource));

                await Task.Delay(delay, cancellationSource.Token);
            }

            await Task.WhenAll(tasks);
        }, cancellationSource.Token);
    }


    private async Task StartProcessorTaskAsync(Channel<PipelineBuffer> readFromChannel, Func<PipelineBuffer, CancellationToken, Task> processorAsync, CancellationTokenSource cancellationTokenSource)
    {
        await Task.Run(async () =>
        {
            var stopwatch = new Stopwatch();
            while (await readFromChannel.Reader.WaitToReadAsync(cancellationTokenSource.Token))
                while (readFromChannel.Reader.TryRead(out var buffer))
                {
                    stopwatch.Restart();
                    try
                    {
                        await processorAsync(buffer, cancellationTokenSource.Token);
                    }
                    catch (Exception e)
                    {
                        Logger.LogError(e, "Failed to execute processorAsync");

                        await TryCloseFileHandlerPoolAsync(buffer.FileHandlerPool);

                        if (cancellationTokenSource.Token.CanBeCanceled)
                        {
                            cancellationTokenSource.Cancel();
                        }

                        throw;
                    }
                    finally
                    {
                        stopwatch.Stop();

                        await UpdateMaxProcessingTimeAsync(stopwatch.Elapsed);
                    }
                }
        }, cancellationTokenSource.Token);
    }

    private async Task UpdateMaxProcessingTimeAsync(TimeSpan stopwatchElapsed)
    {
        await semaphore.WaitAsync();

        try
        {
            if (stopwatchElapsed > currentMaxPartProcessingTime)
            {
                currentMaxPartProcessingTime = stopwatchElapsed;
            }
        }
        finally
        {
            semaphore.Release();
        }
    }

    internal static async Task TryCloseFileHandlerPoolAsync(Channel<FileStream>? fileHandlerPool)
    {
        if (fileHandlerPool is null)
        {
            return;
        }

        if (fileHandlerPool.Writer.TryComplete())
        {
            await foreach (var fileStream in fileHandlerPool.Reader.ReadAllAsync())
            {
                if (!fileStream.SafeFileHandle.IsClosed)
                {
                    fileStream.Close();
                }
            }
        }
    }
}

