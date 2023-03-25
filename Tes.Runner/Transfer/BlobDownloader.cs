// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Net.Http.Headers;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Tes.Runner.Transfer;

public class BlobDownloader : BlobPipeline
{
    public BlobDownloader(BlobPipelineOptions pipelineOptions, Channel<byte[]> memoryBufferPool) : base(pipelineOptions,
        memoryBufferPool)
    {
    }

    public async ValueTask<long> DownloadAsync(List<DownloadInfo>? downloadList)
    {
        ArgumentNullException.ThrowIfNull(downloadList);

        var operationList = downloadList
            .Select(d => new BlobOperationInfo(d.SourceUrl, d.FullFilePath, d.SourceUrl.ToString(), false)).ToList();

        var length = await ExecutePipelineAsync(operationList);

        return length;
    }
    
    protected override async ValueTask<int> ExecuteWriteAsync(PipelineBuffer buffer)
    {
        var fileStream = await buffer.FileHandlerPool.Reader.ReadAsync();

        fileStream.Position = buffer.Offset;

        await fileStream.WriteAsync(buffer.Data, 0, buffer.Length);

        await buffer.FileHandlerPool.Writer.WriteAsync(fileStream);

        return buffer.Length;
    }

    protected override async ValueTask<int> ExecuteReadAsync(PipelineBuffer buffer)
    {
        var request = new HttpRequestMessage(HttpMethod.Get, buffer.BlobUrl);

        request.Headers.Range = new RangeHeaderValue(buffer.Offset, buffer.Offset + buffer.Length);

        var response = await HttpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);

        response.EnsureSuccessStatusCode();

        try
        {
            var data = await response.Content.ReadAsStreamAsync();

            await data.ReadExactlyAsync(buffer.Data, 0, buffer.Length);

            return buffer.Length;
        }
        finally
        {
            response.Dispose();
        }
    }
    
    protected override async Task<long> GetSourceLength(string source)
    {
        var request = new HttpRequestMessage(HttpMethod.Head, new Uri(source));

        var response = await HttpClient.SendAsync(request);

        response.EnsureSuccessStatusCode();

        return response.Content.Headers.ContentLength ?? 0;
    }

    protected override Task OnCompletionAsync(long length, Uri? blobUrl, string fileName)
    {
        Logger.LogInformation($"Completed download. Total:{length} Filename:{fileName}");

        return Task.CompletedTask;
    }
}
