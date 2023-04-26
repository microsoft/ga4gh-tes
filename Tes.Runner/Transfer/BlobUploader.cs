// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Tes.Runner.Transfer
{
    public class BlobUploader : BlobPipeline
    {
        public BlobUploader(BlobPipelineOptions pipelineOptions, Channel<byte[]> memoryBufferPool) : base(pipelineOptions, memoryBufferPool)
        {
        }

        public override void ConfigurePipelineBuffer(PipelineBuffer buffer)
        {
            buffer.BlobPartUrl =
                new Uri($"{buffer.BlobUrl?.AbsoluteUri}&comp=block&blockid={ToBlockId(buffer.Ordinal)}");
        }

        public override async ValueTask<int> ExecuteWriteAsync(PipelineBuffer buffer)
        {
            var request = new HttpRequestMessage(HttpMethod.Put, buffer.BlobPartUrl)
            {
                Content = new ByteArrayContent(buffer.Data, 0, buffer.Length)
            };

            AddPutBlockHeaders(request);

            await ExecuteHttpRequestAsync(request);

            Logger.LogInformation($"Created block:{buffer.BlobPartUrl} ordinal:{buffer.Ordinal} offset:{buffer.Offset} length:{buffer.Length}");

            return buffer.Length;
        }

        private static string ToBlockId(int ordinal)
        {
            return Convert.ToBase64String(Encoding.UTF8.GetBytes($"block{ordinal}"));
        }

        private static void AddPutBlockHeaders(HttpRequestMessage request)
        {
            request.Headers.Add("x-ms-blob-type", "BlockBlob");
            AddBlockBlobServiceHeaders(request);
        }

        private static void AddBlockBlobServiceHeaders(HttpRequestMessage request)
        {
            request.Headers.Add("x-ms-version", "2020-10-02");
            request.Headers.Add("x-ms-date", DateTime.UtcNow.ToString("R"));
        }

        public override async ValueTask<int> ExecuteReadAsync(PipelineBuffer buffer)
        {
            var fileHandler = await buffer.FileHandlerPool.Reader.ReadAsync();

            fileHandler.Position = buffer.Offset;

            var dataRead = await fileHandler.ReadAsync(buffer.Data, 0, buffer.Length);

            await buffer.FileHandlerPool.Writer.WriteAsync(fileHandler);

            return dataRead;
        }

        public override Task<long> GetSourceLengthAsync(string lengthSource)
        {
            return Task.FromResult((new FileInfo(lengthSource)).Length);
        }

        public override async Task OnCompletionAsync(long length, Uri? blobUrl, string fileName)
        {
            ArgumentNullException.ThrowIfNull(blobUrl);

            var content = CreateBlockListContent(length);

            var putBlockUrl = new UriBuilder($"{blobUrl.AbsoluteUri}&comp=blocklist");

            var request = new HttpRequestMessage(HttpMethod.Put, putBlockUrl.Uri)
            {
                Content = content
            };

            AddBlockBlobServiceHeaders(request);

            await ExecuteHttpRequestAsync(request);
        }

        private async ValueTask ExecuteHttpRequestAsync(HttpRequestMessage request)
        {
            var response = await HttpClient.SendAsync(request);

            response.EnsureSuccessStatusCode();
        }

        private StringContent CreateBlockListContent(long length)
        {
            var sb = new StringBuilder();

            sb.Append("<?xml version='1.0' encoding='utf-8'?><BlockList>");

            var parts = BlobSizeUtils.GetNumberOfParts(length, PipelineOptions.BlockSize);

            for (var n = 0; n < parts; n++)
            {
                var blockId = ToBlockId(n);
                sb.Append($"<Latest>{blockId}</Latest>");
            }

            sb.Append("</BlockList>");

            var content = new StringContent(sb.ToString(), Encoding.UTF8, "text/plain");
            return content;
        }

        public async Task<long> UploadAsync(List<UploadInfo>? uploadList)
        {
            ArgumentNullException.ThrowIfNull(uploadList);

            var operationList = uploadList.Select(d => new BlobOperationInfo(d.TargetUri, d.FullFilePath, d.FullFilePath, true)).ToList();

            return await ExecutePipelineAsync(operationList);
        }
    }
}
