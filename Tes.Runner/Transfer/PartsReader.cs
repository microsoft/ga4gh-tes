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

    public PartsReader(IBlobPipeline blobPipeline, BlobPipelineOptions blobPipelineOptions, Channel<byte[]> memoryBufferChannel) :
        base(blobPipeline, blobPipelineOptions, memoryBufferChannel)
    {
    }

    /// <summary>
    /// Starts tasks that perform read operations on the pipeline and writes the part to the write buffer channel.
    /// Once all the parts are read, the write buffer channel is marked as complete.
    /// The number of tasks is determined by the <see cref="BlobPipelineOptions.NumberOfReaders"/> option.
    /// </summary>
    /// <param name="readBufferChannel">Source channel from which the parts are read</param>
    /// <param name="writeBufferChannel">Target channel where read parts are written</param>
    /// <returns>A tasks that completes when all tasks complete</returns>
    public async Task StartPartsReaderAsync(Channel<PipelineBuffer> readBufferChannel, Channel<PipelineBuffer> writeBufferChannel)
    {
        var tasks = new List<Task>();

        for (int i = 0; i < BlobPipelineOptions.NumberOfReaders; i++)
        {
            tasks.Add(Task.Run(async () =>
                {
                    PipelineBuffer? buffer;

                    while (await readBufferChannel.Reader.WaitToReadAsync())
                        while (readBufferChannel.Reader.TryRead(out buffer))
                        {
                            try
                            {
                                buffer.Data = await MemoryBufferChannel.Reader.ReadAsync();

                                await BlobPipeline.ExecuteReadAsync(buffer);

                                await writeBufferChannel.Writer.WriteAsync(buffer);
                            }
                            catch (Exception e)
                            {
                                logger.LogError(e, "Failed to execute read operation.");
                                throw;
                            }
                        }
                }
            ));
        }

        await Task.WhenAll(tasks);

        writeBufferChannel.Writer.Complete();

        logger.LogInformation("All part read operations completed successfully.");
    }
}
