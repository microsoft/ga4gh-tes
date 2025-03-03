// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Tes.Runner.Transfer;

/// <summary>
/// Handles the creation of tasks that perform read operations on the pipeline. 
/// </summary>
public class PartsReader : PartsProcessor
{
    private readonly ILogger logger = PipelineLoggerFactory.Create<PartsReader>();

    public PartsReader(IBlobPipeline blobPipeline, BlobPipelineOptions blobPipelineOptions, Channel<byte[]> memoryBufferChannel, IScalingStrategy scalingStrategy) :
        base(blobPipeline, blobPipelineOptions, memoryBufferChannel, scalingStrategy)
    {
    }

    /// <summary>
    /// Starts tasks that perform read operations on the pipeline and writes the part to the write buffer channel.
    /// Once all the parts are read, the write buffer channel is marked as complete.
    /// The number of tasks is determined by the <see cref="BlobPipelineOptions.NumberOfReaders"/> option.
    /// </summary>
    /// <param name="readBufferChannel">Source channel from which the parts are read</param>
    /// <param name="writeBufferChannel">Target channel where read parts are written</param>
    /// <param name="cancellationSource">Cancellation source used to cancel working threads if an error occurs</param>
    /// <returns>A tasks that completes when all tasks complete</returns>
    public async Task StartPartsReaderAsync(Channel<PipelineBuffer> readBufferChannel, Channel<PipelineBuffer> writeBufferChannel, CancellationTokenSource cancellationSource)
    {
        async Task ReadPartAsync(PipelineBuffer buffer, CancellationToken cancellationToken)
        {
            buffer.Data = await MemoryBufferChannel.Reader.ReadAsync(cancellationToken);

            await BlobPipeline.ExecuteReadAsync(buffer, cancellationToken);

            await writeBufferChannel.Writer.WriteAsync(buffer, cancellationToken);
        }


        try
        {
            await StartProcessorsWithScalingStrategyAsync(BlobPipelineOptions.NumberOfReaders, readBufferChannel, ReadPartAsync, cancellationSource);
        }
        catch (Exception e)
        {
            logger.LogError(e, "Error reading parts from the pipeline.");
            throw;
        }
        finally
        {
            writeBufferChannel.Writer.Complete();
        }

        logger.LogDebug("All part read operations completed successfully.");
    }
}
