﻿// Copyright (c) Microsoft Corporation.
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
        async Task WritePartAsync(PipelineBuffer buffer, CancellationToken cancellationToken)
        {
            await BlobPipeline.ExecuteWriteAsync(buffer, cancellationToken);

            await processedBufferChannel.Writer.WriteAsync(ToProcessedBuffer(buffer), cancellationToken);

            await MemoryBufferChannel.Writer.WriteAsync(buffer.Data, cancellationToken);
        }

        var tasks = StartProcessors(BlobPipelineOptions.NumberOfWriters, writeBufferChannel, WritePartAsync);

        try
        {
            await Task.WhenAll(tasks);
        }
        catch (Exception e)
        {
            logger.LogError(e, "Error writing parts to the pipeline.");
            throw;
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
