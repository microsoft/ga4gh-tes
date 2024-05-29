// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Tes.Runner.Transfer;

/// <summary>
/// Blob operation pipeline for downloading files from an HTTP source.
/// </summary>
public class BlobDownloader : BlobOperationPipeline
{
    public BlobDownloader(
        BlobPipelineOptions pipelineOptions,
        Channel<byte[]> memoryBufferPool,
        Func<IBlobPipeline, ProcessedPartsProcessor> processedPartsProcessorFactory,
        Func<IBlobPipeline, BlobPipelineOptions, PartsProducer> partsProducerFactory,
        Func<IBlobPipeline, BlobPipelineOptions, Channel<byte[]>, IScalingStrategy, PartsWriter> partsWriterFactory,
        Func<IBlobPipeline, BlobPipelineOptions, Channel<byte[]>, IScalingStrategy, PartsReader> partsReaderFactory,
        ILogger<BlobDownloader> logger)
        : base(pipelineOptions, memoryBufferPool, processedPartsProcessorFactory, partsProducerFactory, partsWriterFactory, partsReaderFactory, logger)
    { }

    /// <summary>
    /// Parameter-less constructor for mocking
    /// </summary>
    protected BlobDownloader()
        : base(
            new BlobPipelineOptions(),
            Channel.CreateUnbounded<byte[]>(),
            pipeLine => new ProcessedPartsProcessor(pipeLine, Microsoft.Extensions.Logging.Abstractions.NullLogger<ProcessedPartsProcessor>.Instance),
            (pipeLine, pipelineOptions) => new PartsProducer(pipeLine, pipelineOptions, Microsoft.Extensions.Logging.Abstractions.NullLogger<PartsProducer>.Instance),
            (pipeLine, pipelineOptions, memoryBuffer, scalingStrategy) => new PartsWriter(pipeLine, pipelineOptions, memoryBuffer, scalingStrategy, Microsoft.Extensions.Logging.Abstractions.NullLogger<PartsWriter>.Instance),
            (pipeLine, pipelineOptions, memoryBuffer, scalingStrategy) => new PartsReader(pipeLine, pipelineOptions, memoryBuffer, scalingStrategy, Microsoft.Extensions.Logging.Abstractions.NullLogger<PartsReader>.Instance),
            Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance)
    { }

    /// <summary>
    /// Downloads a list of files from an HTTP source.
    /// </summary>
    /// <param name="downloadList">A list of <see cref="DownloadInfo"/></param>
    /// <returns>Total bytes downloaded</returns>
    public virtual async ValueTask<long> DownloadAsync(List<DownloadInfo> downloadList)
    {
        ValidateDownloadList(downloadList);

        var operationList = downloadList
            .Select(d => new BlobOperationInfo(d.SourceUrl, d.FullFilePath, d.SourceUrl.ToString(), false)).ToList();

        var length = await ExecutePipelineAsync(operationList);

        return length;
    }

    /// <summary>
    /// Writes the part's data to the target file.
    /// </summary>
    /// <param name="buffer">Part's data <see cref="PipelineBuffer"/></param>
    /// <param name="cancellationToken"></param>
    /// <returns>Part's size in bytes</returns>
    public override async ValueTask<int> ExecuteWriteAsync(PipelineBuffer buffer, CancellationToken cancellationToken)
    {
        var fileStream = await buffer.FileHandlerPool.Reader.ReadAsync(cancellationToken);

        fileStream.Position = buffer.Offset;

        await fileStream.WriteAsync(buffer.Data.AsMemory(0, buffer.Length), cancellationToken);

        await buffer.FileHandlerPool.Writer.WriteAsync(fileStream, cancellationToken);

        return buffer.Length;
    }

    /// <summary>
    /// Reads part's data from the file requesting the data by range.
    /// </summary>
    /// <param name="buffer"><see cref="PipelineBuffer"/> where to write the part's data</param>
    /// <param name="cancellationToken"></param>
    /// <returns>Part's length in bytes</returns>
    public override async ValueTask<int> ExecuteReadAsync(PipelineBuffer buffer, CancellationToken cancellationToken)
    {

        if (buffer.Length == 0)
        {
            return 0;
        }

        return await BlobApiHttpUtils.ExecuteHttpRequestAndReadBodyResponseAsync(buffer, () => BlobApiHttpUtils.CreateReadByRangeHttpRequest(buffer), cancellationToken);
    }

    /// <summary>
    /// Get the size of the file by making an HTTP HEAD request.
    /// </summary>
    /// <param name="source">URL to the file</param>
    /// <returns>File's size</returns>
    public override async Task<long> GetSourceLengthAsync(string source)
    {
        HttpResponseMessage? response = null;

        try
        {
            response = await BlobApiHttpUtils.ExecuteHttpRequestAsync(() => new HttpRequestMessage(HttpMethod.Head, new Uri(source)));

            return response.Content.Headers.ContentLength ?? 0;
        }
        finally
        {
            response?.Dispose();
        }
    }

    /// <summary>
    /// No-op for the downloader.
    /// </summary>
    /// <param name="length"></param>
    /// <param name="blobUrl"></param>
    /// <param name="fileName"></param>
    /// <param name="rootHash"></param>
    /// <returns></returns>
    public override Task OnCompletionAsync(long length, Uri? blobUrl, string fileName, string? rootHash)
    {
        Logger.LogInformation("Completed download. Total bytes: {FileSize:n0} Filename: {FileName}", length, fileName);

        return Task.CompletedTask;
    }

    /// <summary>
    /// No-op for the downloader.
    /// </summary>
    /// <param name="buffer"></param>
    public override void ConfigurePipelineBuffer(PipelineBuffer buffer)
    {
        //no config
    }

    private static void ValidateDownloadList(List<DownloadInfo> downloadList)
    {
        ArgumentNullException.ThrowIfNull(downloadList);

        if (downloadList.Count == 0)
        {
            throw new ArgumentException("Download list is empty", nameof(downloadList));
        }

        foreach (var downloadInfo in downloadList)
        {
            if (string.IsNullOrEmpty(downloadInfo.FullFilePath))
            {
                throw new ArgumentException("Full file path is empty for one of the items in the list");
            }
            if (downloadInfo.SourceUrl == null)
            {
                throw new ArgumentException("Source URL is empty for one of the items in the list");
            }
        }
    }
}
