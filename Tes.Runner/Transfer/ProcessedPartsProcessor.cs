using System.Threading.Channels;

namespace Tes.Runner.Transfer;

public class ProcessedPartsProcessor
{
    private readonly IBlobPipeline blobPipeline;

    public ProcessedPartsProcessor(IBlobPipeline blobPipeline)
    {
        this.blobPipeline = blobPipeline;
    }

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
        {
            while (processedBufferChannel.Reader.TryRead(out buffer))
            {
                totalBytes += buffer.Length;

                if (!partsProcessed.ContainsKey(buffer.FileName))
                {
                    partsProcessed.Add(buffer.FileName, 1);
                    continue;
                }

                var total = ++partsProcessed[buffer.FileName];

                if (total == buffer.NumberOfParts)
                {
                    tasks.Add(blobPipeline.OnCompletionAsync(buffer.FileSize, buffer.BlobUrl, buffer.FileName));

                    processedFiles++;

                    await CloseFileHandlerPoolAsync(buffer.FileHandlerPool);

                    allFilesProcessed = processedFiles == expectedNumberOfFiles;
                }
            }
        }

        await Task.WhenAll(tasks);

        readBufferChannel.Writer.Complete();

        return totalBytes;
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
