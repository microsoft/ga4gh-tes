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
        var cancellationTokenSource = new CancellationTokenSource();

        while (!allFilesProcessed && await processedBufferChannel.Reader.WaitToReadAsync(cancellationTokenSource.Token))
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
                    tasks.Add(CompleteFileProcessingAsync(buffer, cancellationTokenSource));

                    processedFiles++;

                    allFilesProcessed = processedFiles == expectedNumberOfFiles;
                }
            }

        try
        {
            await Task.WhenAll(tasks);
        }
        finally
        {
            readBufferChannel.Writer.Complete();
        }

        logger.LogInformation("All parts were successfully processed.");

        return totalBytes;
    }

    private async Task CompleteFileProcessingAsync(ProcessedBuffer buffer, CancellationTokenSource cancellationTokenSource)
    {
        try
        {
            var rootHash = string.Empty;

            if (buffer.HashListProvider is not null)
            {
                rootHash = GetRootHash(buffer.HashListProvider);
            }

            await blobPipeline.OnCompletionAsync(buffer.FileSize, buffer.BlobUrl, buffer.FileName, rootHash);

            await CloseFileHandlerPoolAsync(buffer.FileHandlerPool, cancellationTokenSource.Token);
        }
        catch (Exception e)
        {
            logger.LogError(e, $"Failed to complete the file operation. File name: {buffer.FileName}");

            if (!cancellationTokenSource.IsCancellationRequested)
            {
                logger.LogDebug("Cancelling tasks in the processed parts processor.");
                cancellationTokenSource.Cancel();
            }
            throw;
        }
    }

    private string GetRootHash(IHashListProvider hashListProvider)
    {
        return hashListProvider.GetRootHash();
    }

    private async ValueTask CloseFileHandlerPoolAsync(Channel<FileStream> bufferFileHandlerPool, CancellationToken cancellationToken)
    {
        if (bufferFileHandlerPool.Writer.TryComplete())
        {
            await foreach (var fileStream in bufferFileHandlerPool.Reader.ReadAllAsync(cancellationToken))
            {
                CloseFileHandler(fileStream);
            }
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
