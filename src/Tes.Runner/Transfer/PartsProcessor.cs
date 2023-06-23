// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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
    private readonly ILogger logger = PipelineLoggerFactory.Create<PartsProcessor>();

    protected PartsProcessor(IBlobPipeline blobPipeline, BlobPipelineOptions blobPipelineOptions, Channel<byte[]> memoryBufferChannel)
    {
        ValidateArguments(blobPipeline, blobPipelineOptions, memoryBufferChannel);

        BlobPipeline = blobPipeline;
        BlobPipelineOptions = blobPipelineOptions;
        MemoryBufferChannel = memoryBufferChannel;
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

    public static async Task WhenAllOrThrowIfOneFailsAsync(List<Task> tasks)
    {
        var tasksPending = tasks.ToList();
        while (tasksPending.Any())
        {
            var completedTask = await Task.WhenAny(tasksPending);

            tasksPending.Remove(completedTask);

            if (completedTask.IsFaulted)
            {
                throw new InvalidOperationException("At least one of the tasks has failed.", completedTask.Exception);
            }

            if (completedTask.IsCanceled)
            {
                throw new InvalidOperationException("At least one of the tasks has been canceled.", completedTask.Exception);
            }
        }
    }

    protected List<Task> StartProcessors(int numberOfProcessors, Channel<PipelineBuffer> readFromChannel, Func<PipelineBuffer, CancellationToken, Task> processorAsync)
    {
        ArgumentNullException.ThrowIfNull(readFromChannel);
        ArgumentNullException.ThrowIfNull(processorAsync);

        var cancellationTokenSource = new CancellationTokenSource();

        var tasks = new List<Task>();
        for (var i = 0; i < numberOfProcessors; i++)
        {
            tasks.Add(Task.Run(async () =>
            {
                PipelineBuffer? buffer;
                while (await readFromChannel.Reader.WaitToReadAsync(cancellationTokenSource.Token))
                    while (readFromChannel.Reader.TryRead(out buffer))
                    {
                        try
                        {
                            await processorAsync(buffer, cancellationTokenSource.Token);
                        }
                        catch (Exception e)
                        {
                            logger.LogError(e, "Failed to execute processorAsync");
                            if (cancellationTokenSource.Token.CanBeCanceled)
                            {
                                cancellationTokenSource.Cancel();
                            }
                            throw;
                        }
                    }
            }, cancellationTokenSource.Token));
        }
        return tasks;
    }
}
