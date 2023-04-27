// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Tes.Runner.Transfer;

/// <summary>
/// This class is responsible for creating parts in the pipeline.
/// </summary>
public class PartsProducer
{
    private readonly IBlobPipeline blobPipeline;
    private readonly ILogger logger = PipelineLoggerFactory.Create<PartsProducer>();
    private readonly BlobPipelineOptions blobPipelineOptions;


    public PartsProducer(IBlobPipeline blobPipeline, BlobPipelineOptions blobPipelineOptions)
    {
        ArgumentNullException.ThrowIfNull(blobPipelineOptions);
        ArgumentNullException.ThrowIfNull(blobPipeline);

        this.blobPipeline = blobPipeline;
        this.blobPipelineOptions = blobPipelineOptions;
    }
    /// <summary>
    /// Starts Tasks that produce parts for each blob operation.
    /// This operation completes when all parts are written to the pipeline buffer channel.
    /// </summary>
    /// <param name="blobOperations">A list of <see cref="BlobOperationInfo"/>></param>
    /// <param name="readBufferChannel">Channel where are parts are written</param>
    /// <returns></returns>
    public async Task StartPartsProducersAsync(List<BlobOperationInfo> blobOperations, Channel<PipelineBuffer> readBufferChannel)
    {
        ValidateOperations(blobOperations, readBufferChannel);

        var partsProducerTasks = new List<Task>();
        foreach (var operation in blobOperations)
        {
            partsProducerTasks.Add(StartPartsProducerAsync(operation, readBufferChannel));
        }

        try
        {
            await Task.WhenAll(partsProducerTasks);
            logger.LogInformation("All part producing operations are complete.");

        }
        catch (Exception e)
        {
            logger.LogError(e, "The parts producer failed.");
            throw;
        }
    }

    private static void ValidateOperations(List<BlobOperationInfo> blobOperations, Channel<PipelineBuffer> readBufferChannel)
    {
        ArgumentNullException.ThrowIfNull(blobOperations);
        ArgumentNullException.ThrowIfNull(readBufferChannel);

        if (blobOperations.Count == 0)
        {
            throw new ArgumentException("No blob operations were provided.");
        }

        var duplicateEntries = blobOperations
            .GroupBy(b => b.FileName)
            .Any(i => i.Count() > 1);

        if (duplicateEntries)
        {
            throw new ArgumentException(
                "Duplicate entries found in the list of blob operations. The filename must be unique.");
        }
    }

    private async Task StartPartsProducerAsync(BlobOperationInfo operation, Channel<PipelineBuffer> readBufferChannel)
    {
        var length = await blobPipeline.GetSourceLengthAsync(operation.SourceLocationForLength);

        var numberOfParts = BlobSizeUtils.GetNumberOfParts(length, blobPipelineOptions.BlockSize);

        var fileHandlerPool = await GetNewFileHandlerPoolAsync(operation.FileName, operation.ReadOnlyHandlerForExistingFile);

        if (length == 0)
        {
            await CreateAndWritePipelinePartBufferAsync(operation, readBufferChannel, fileHandlerPool, length, partOrdinal: 0, numberOfParts: 1);
            return;
        }

        for (var i = 0; i < numberOfParts; i++)
        {
            await CreateAndWritePipelinePartBufferAsync(operation, readBufferChannel, fileHandlerPool, length, partOrdinal: i, numberOfParts);
        }
    }

    private async Task CreateAndWritePipelinePartBufferAsync(BlobOperationInfo operation, Channel<PipelineBuffer> readBufferChannel,
        Channel<FileStream> fileHandlerPool, long length, int partOrdinal, int numberOfParts)
    {
        PipelineBuffer buffer;
        buffer = GetNewPipelinePartBuffer(operation.Url, operation.FileName, fileHandlerPool, length, partOrdinal, numberOfParts);

        await readBufferChannel.Writer.WriteAsync(buffer);
    }

    private PipelineBuffer GetNewPipelinePartBuffer(Uri blobUrl, string fileName, Channel<FileStream> fileHandlerPool,
        long fileSize, int partOrdinal,
        int numberOfParts)
    {
        var buffer = new PipelineBuffer()
        {
            BlobUrl = blobUrl,
            Offset = (long)partOrdinal * blobPipelineOptions.BlockSize,
            Length = blobPipelineOptions.BlockSize,
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

        blobPipeline.ConfigurePipelineBuffer(buffer);

        return buffer;
    }

    private async ValueTask<Channel<FileStream>> GetNewFileHandlerPoolAsync(string fileName, bool readOnly)
    {
        var pool = Channel.CreateBounded<FileStream>(blobPipelineOptions.BufferCapacity);

        for (int f = 0; f < blobPipelineOptions.BufferCapacity; f++)
        {
            try
            {
                if (readOnly)
                {
                    //when readonly we are assuming the file already exist so no need to create directory and the file
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
            catch (Exception e)
            {
                logger.LogError(e, $"Failed to open the file or create directory. File name {fileName}");
                throw;
            }
        }

        return pool;
    }
}
