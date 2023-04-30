// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Tes.Runner.Transfer;

public class BlobDownloader : BlobOperationPipeline
{
    public BlobDownloader(BlobPipelineOptions pipelineOptions, Channel<byte[]> memoryBufferPool) : base(pipelineOptions,
        memoryBufferPool)
    {
    }

    public async ValueTask<long> DownloadAsync(List<DownloadInfo> downloadList)
    {
        ArgumentNullException.ThrowIfNull(downloadList);

        var operationList = downloadList
            .Select(d => new BlobOperationInfo(d.SourceUrl, d.FullFilePath, d.SourceUrl.ToString(), false)).ToList();

        var length = await ExecutePipelineAsync(operationList);

        return length;
    }

    public override async ValueTask<int> ExecuteWriteAsync(PipelineBuffer buffer)
    {
        var fileStream = await buffer.FileHandlerPool.Reader.ReadAsync();

        fileStream.Position = buffer.Offset;

        await fileStream.WriteAsync(buffer.Data, 0, buffer.Length);

        await buffer.FileHandlerPool.Writer.WriteAsync(fileStream);

        return buffer.Length;
    }

    public override async ValueTask<int> ExecuteReadAsync(PipelineBuffer buffer)
    {
        var request = BlobBlockApiHttpUtils.CreateReadByRangeHttpRequest(buffer);

        return await BlobBlockApiHttpUtils.ExecuteHttpRequestAndReadBodyResponseAsync(buffer, request);
    }

    public override async Task<long> GetSourceLengthAsync(string source)
    {
        var request = new HttpRequestMessage(HttpMethod.Head, new Uri(source));

        var response = await BlobBlockApiHttpUtils.ExecuteHttpRequestAsync(request);

        //var response = await HttpClient.SendAsync(request);

        response.EnsureSuccessStatusCode();

        return response.Content.Headers.ContentLength ?? 0;
    }

    public override Task OnCompletionAsync(long length, Uri? blobUrl, string fileName)
    {
        Logger.LogInformation($"Completed download. Total:{length} Filename:{fileName}");

        return Task.CompletedTask;
    }

    public override void ConfigurePipelineBuffer(PipelineBuffer buffer)
    {
        //no config
    }
}
