// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Tes.Runner.Transfer;
/// <summary>
/// Handles the processed parts.
/// </summary>
public class ProcessedPartsProcessor
{
    private readonly ILogger logger = PipelineLoggerFactory.Create<ProcessedPartsProcessor>();

    private readonly IBlobPipeline blobPipeline;

    public ProcessedPartsProcessor(IBlobPipeline blobPipeline)
    {
        this.blobPipeline = blobPipeline;
    }

    /// <summary>
    /// Starts a single-threaded process that reads the processed buffers from the channel and keeps track of the number of parts processed.
    /// When all the parts for a file are processed, the operation is marked as completed.
    /// </summary>
    /// <param name="expectedNumberOfFiles"></param>
    /// <param name="processedBufferChannel"></param>
    /// <param name="readBufferChannel"></param>
    /// <returns></returns>
    public async ValueTask<long> StartProcessedPartsProcessorAsync(int expectedNumberOfFiles,
        Channel<ProcessedBuffer> processedBufferChannel, Channel<PipelineBuffer> readBufferChannel)
    {
        var tasks = new List<Task>();

        ProcessedBuffer? buffer;
        var partsProcessed = new Dictionary<string, int>();
        var allFilesProcessed = false;
        var processedFiles = 0;
        long totalBytes = 0;

        while (!allFilesProcessed && await processedBufferChannel.Reader.WaitToReadAsync())
            while (processedBufferChannel.Reader.TryRead(out buffer))
            {
                totalBytes += buffer.Length;

                if (!partsProcessed.ContainsKey(buffer.FileName))
                {
                    partsProcessed.Add(buffer.FileName, 0);
                }

                var total = ++partsProcessed[buffer.FileName];

                if (total == buffer.NumberOfParts)
                {
                    var rootHash = string.Empty;

                    if (buffer.HashListProvider is not null)
                    {
                        rootHash = GetRootHashIfProviderIsSet(buffer.HashListProvider);
                    }

                    tasks.Add(blobPipeline.OnCompletionAsync(buffer.FileSize, buffer.BlobUrl, buffer.FileName, rootHash));

                    processedFiles++;

                    await CloseFileHandlerPoolAsync(buffer.FileHandlerPool);

                    allFilesProcessed = processedFiles == expectedNumberOfFiles;
                }
            }

        try
        {
            await PartsProcessor.WhenAllOrThrowIfOneFailsAsync(tasks);
        }
        finally
        {
            readBufferChannel.Writer.Complete();
        }

        logger.LogInformation("All parts were successfully processed.");

        return totalBytes;
    }

    private string GetRootHashIfProviderIsSet(IHashListProvider hashListProvider)
    {
        return hashListProvider.GetRootHash();
    }

    private async ValueTask CloseFileHandlerPoolAsync(Channel<FileStream> bufferFileHandlerPool)
    {
        bufferFileHandlerPool.Writer.Complete();

        await foreach (FileStream fileStream in bufferFileHandlerPool.Reader.ReadAllAsync())
        {
            CloseFileHandler(fileStream);
        }
    }

    private void CloseFileHandler(FileStream? fileStream)
    {
        if (fileStream is not null && !fileStream.SafeFileHandle.IsClosed)
        {
            fileStream.Close();
        }
    }
}
