// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Tes.Runner.Transfer;

public class PartsReader : PartsProcessor
{
    private readonly ILogger logger = PipelineLoggerFactory.Create<PartsReader>();

    public PartsReader(IBlobPipeline blobPipeline, BlobPipelineOptions blobPipelineOptions, Channel<byte[]> memoryBufferChannel) :
        base(blobPipeline, blobPipelineOptions, memoryBufferChannel)
    {
    }

    public async Task StartPartsReaderAsync(Channel<PipelineBuffer> readBufferChannel, Channel<PipelineBuffer> writeBufferChannel)
    {
        var tasks = new List<Task>();

        for (int i = 0; i < BlobPipelineOptions.NumberOfReaders; i++)
        {
            tasks.Add(Task.Run(async () =>
                {
                    PipelineBuffer? buffer;

                    while (await readBufferChannel.Reader.WaitToReadAsync())
                    {
                        while (readBufferChannel.Reader.TryRead(out buffer))
                        {
                            try
                            {
                                buffer.Data = await MemoryBufferChannel.Reader.ReadAsync();

                                logger.LogDebug("Executing read operation.");
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
                }
            ));
        }

        await Task.WhenAll(tasks);
        logger.LogInformation("All part read operations are complete.");
        writeBufferChannel.Writer.Complete();
    }
}
