// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Tes.Runner.Transfer;

/// <summary>
/// Creates parts in the pipeline from the provided operations.
/// </summary>
public class PartsProducer
{
    private readonly IBlobPipeline blobPipeline;
    private readonly ILogger logger = PipelineLoggerFactory.Create<PartsProducer>();
    private readonly BlobPipelineOptions blobPipelineOptions;
    private readonly List<Channel<FileStream>> fileHandlersPools = new List<Channel<FileStream>>();


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
    /// <param name="cancellationSource"></param>
    /// <returns></returns>
    public async Task StartPartsProducersAsync(List<BlobOperationInfo> blobOperations, Channel<PipelineBuffer> readBufferChannel, CancellationTokenSource cancellationSource)
    {
        ValidateOperations(blobOperations, readBufferChannel);

        var partsProducerTasks = new List<Task>();

        foreach (var operation in blobOperations)
        {
            partsProducerTasks.Add(StartPartsProducerAsync(operation, readBufferChannel, cancellationSource));
        }

        try
        {
            await Task.WhenAll(partsProducerTasks);

            logger.LogDebug("All parts from requested operations were created.");
        }
        catch (Exception e)
        {
            logger.LogError(e, "The parts producer failed.");

            await TryCloseAllFileHandlerPools();

            throw;
        }
    }

    private async Task TryCloseAllFileHandlerPools()
    {
        foreach (var fileHandlePool in fileHandlersPools)
        {
            await PartsProcessor.TryCloseFileHandlerPoolAsync(fileHandlePool);
        }
    }

    private static void ValidateOperations(List<BlobOperationInfo> blobOperations, Channel<PipelineBuffer> readBufferChannel)
    {
        ArgumentNullException.ThrowIfNull(blobOperations);
        ArgumentNullException.ThrowIfNull(readBufferChannel);

        if (blobOperations.Count == 0)
        {
            throw new ArgumentException("No blob operations were provided.", nameof(blobOperations));
        }

        var duplicateEntries = blobOperations
            .GroupBy(b => b.FileName)
            .Any(i => i.Count() > 1);

        if (duplicateEntries)
        {
            throw new ArgumentException(
                "Duplicate entries found in the list of blob operations. The filename must be unique.", nameof(blobOperations));
        }
    }

    private async Task StartPartsProducerAsync(BlobOperationInfo operation, Channel<PipelineBuffer> readBufferChannel, CancellationTokenSource cancellationTokenSource)
    {
        try
        {
            var length = await blobPipeline.GetSourceLengthAsync(operation.SourceLocationForLength);

            var numberOfParts = BlobSizeUtils.GetNumberOfParts(length, blobPipelineOptions.BlockSizeBytes);

            var fileHandlerPool = await GetNewFileHandlerPoolAsync(operation.FileName, operation.ReadOnlyHandlerForExistingFile, cancellationTokenSource.Token);

            if (length == 0)
            {
                await CreateAndWritePipelinePartBufferAsync(operation, readBufferChannel, fileHandlerPool, length, partOrdinal: 0, numberOfParts: 1, cancellationTokenSource.Token);
                return;
            }

            for (var i = 0; i < numberOfParts; i++)
            {
                await CreateAndWritePipelinePartBufferAsync(operation, readBufferChannel, fileHandlerPool, length, partOrdinal: i, numberOfParts, cancellationTokenSource.Token);
            }

        }
        catch (Exception e)
        {
            logger.LogError(e, $"Failed to create parts for file {operation.FileName}");
            if (!cancellationTokenSource.IsCancellationRequested)
            {
                cancellationTokenSource.Cancel();
            }
            throw;
        }
    }

    private async Task CreateAndWritePipelinePartBufferAsync(BlobOperationInfo operation, Channel<PipelineBuffer> readBufferChannel,
        Channel<FileStream> fileHandlerPool, long length, int partOrdinal, int numberOfParts, CancellationToken cancellationToken)
    {
        PipelineBuffer buffer;
        buffer = GetNewPipelinePartBuffer(operation.Url, operation.FileName, fileHandlerPool, length, partOrdinal, numberOfParts);

        await readBufferChannel.Writer.WriteAsync(buffer, cancellationToken);
    }

    private PipelineBuffer GetNewPipelinePartBuffer(Uri blobUrl, string fileName, Channel<FileStream> fileHandlerPool,
        long fileSize, int partOrdinal,
        int numberOfParts)
    {
        var buffer = new PipelineBuffer()
        {
            BlobUrl = blobUrl,
            Offset = (long)partOrdinal * blobPipelineOptions.BlockSizeBytes,
            Length = blobPipelineOptions.BlockSizeBytes,
            FileName = fileName,
            Ordinal = partOrdinal,
            NumberOfParts = numberOfParts,
            FileHandlerPool = fileHandlerPool,
            FileSize = fileSize
        };

        if (partOrdinal == numberOfParts - 1)
        {
            buffer.Length = (int)(fileSize - buffer.Offset);
        }

        blobPipeline.ConfigurePipelineBuffer(buffer);

        return buffer;
    }

    private async ValueTask<Channel<FileStream>> GetNewFileHandlerPoolAsync(string fileName, bool readOnly, CancellationToken cancellationToken)
    {
        var pool = Channel.CreateBounded<FileStream>(blobPipelineOptions.FileHandlerPoolCapacity);

        for (var f = 0; f < blobPipelineOptions.FileHandlerPoolCapacity; f++)
        {
            try
            {
                if (readOnly)
                {
                    //when readonly we are assuming the file already exist so no need to create directory and the file
                    await pool.Writer.WriteAsync(File.Open(fileName, FileMode.Open, FileAccess.Read, FileShare.Read), cancellationToken);
                    continue;
                }

                CreateDirectoryIfNotPresent(fileName);

                await pool.Writer.WriteAsync(File.Open(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite), cancellationToken);
            }
            catch (Exception e)
            {
                logger.LogError(e, $"Failed to open the file or create directory. File name {fileName}");
                throw;
            }
        }

        fileHandlersPools.Add(pool);

        return pool;
    }

    private static void CreateDirectoryIfNotPresent(string fileName)
    {
        var directory = Path.GetDirectoryName(fileName);

        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }
    }
}
