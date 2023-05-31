// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Tes.Runner.Transfer;

/// <summary>
/// Handles the creation of tasks that perform write operation on the pipeline.
/// </summary>
public class PartsWriter : PartsProcessor
{
    private readonly ILogger logger = PipelineLoggerFactory.Create<PartsWriter>();

    public PartsWriter(IBlobPipeline blobPipeline, BlobPipelineOptions blobPipelineOptions, Channel<byte[]> memoryBufferChannel) : base(blobPipeline, blobPipelineOptions, memoryBufferChannel)
    {
    }

    /// <summary>
    /// Starts a number of parallel writer tasks that read from the write buffer channel and execute the write operation in the pipeline.
    /// The number of tasks is determined by the <see cref="BlobPipelineOptions.NumberOfWriters"/> option.
    /// Once the write operation is complete, a processed buffer is written to the processed buffer channel.
    /// The processed buffer channel is complete/closed when all write operations are complete.
    /// </summary>
    /// <param name="writeBufferChannel">Source channel from which the parts are read to perform the write operation on the pipeline</param>
    /// <param name="processedBufferChannel">Target channel where processed parts are written</param>
    /// <returns>A task that completes when all the writer tasks complete</returns>
    public async Task StartPartsWritersAsync(Channel<PipelineBuffer> writeBufferChannel, Channel<ProcessedBuffer> processedBufferChannel)
    {
        ArgumentNullException.ThrowIfNull(writeBufferChannel);
        ArgumentNullException.ThrowIfNull(processedBufferChannel);

        var tasks = new List<Task>();

        for (int i = 0; i < BlobPipelineOptions.NumberOfWriters; i++)
        {
            tasks.Add(Task.Run(async () =>
            {
                PipelineBuffer? buffer;

                while (await writeBufferChannel.Reader.WaitToReadAsync())
                    while (writeBufferChannel.Reader.TryRead(out buffer))
                    {
                        try
                        {
                            await BlobPipeline.ExecuteWriteAsync(buffer);

                            await processedBufferChannel.Writer.WriteAsync(ToProcessedBuffer(buffer));

                            await MemoryBufferChannel.Writer.WriteAsync(buffer.Data);
                        }
                        catch (Exception e)
                        {
                            logger.LogError(e, "Failed to execute write operation");
                            throw;
                        }
                    }
            }));
        }

        try
        {
            await WhenAllOrThrowIfOneFailsAsync(tasks);
        }
        finally
        {
            processedBufferChannel.Writer.Complete();
        }

        logger.LogInformation("All part write operations completed successfully.");
    }

    private ProcessedBuffer ToProcessedBuffer(PipelineBuffer buffer)
    {
        return new ProcessedBuffer(buffer.FileName, buffer.BlobUrl, buffer.FileSize, buffer.Ordinal,
            buffer.NumberOfParts, buffer.FileHandlerPool, buffer.BlobPartUrl, buffer.Length, buffer.HashListProvider);
    }
}
