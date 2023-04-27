// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;

namespace Tes.Runner.Transfer
{
    /// <summary>
    /// Blob operation pipeline for uploading blobs.
    /// </summary>
    public class BlobUploader : BlobOperationPipeline
    {
        public BlobUploader(BlobPipelineOptions pipelineOptions, Channel<byte[]> memoryBufferPool) : base(pipelineOptions, memoryBufferPool)
        {
        }

        public override void ConfigurePipelineBuffer(PipelineBuffer buffer)
        {
            buffer.BlobPartUrl =
                BlobBlockApiHttpUtils.ParsePutBlockUrl(buffer.BlobPartUrl, buffer.Ordinal);
        }

        public override async ValueTask<int> ExecuteWriteAsync(PipelineBuffer buffer)
        {
            var request = BlobBlockApiHttpUtils.CreatePutBlockRequestAsync(buffer, PipelineOptions.ApiVersion);

            await ExecuteHttpRequestAsync(request);

            return buffer.Length;
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

            var request = BlobBlockApiHttpUtils.CreateBlobBlockListRequest(length, blobUrl, PipelineOptions.BlockSize);

            await ExecuteHttpRequestAsync(request);
        }

        private async ValueTask ExecuteHttpRequestAsync(HttpRequestMessage request)
        {
            var response = await HttpClient.SendAsync(request);

            response.EnsureSuccessStatusCode();
        }

        public async Task<long> UploadAsync(List<UploadInfo> uploadList)
        {
            ValidateUploadList(uploadList);

            var operationList = uploadList.Select(d => new BlobOperationInfo(d.TargetUri, d.FullFilePath, d.FullFilePath, true)).ToList();

            return await ExecutePipelineAsync(operationList);
        }

        private static void ValidateUploadList(List<UploadInfo> uploadList)
        {
            ArgumentNullException.ThrowIfNull(uploadList);

            if (uploadList.Count == 0)
            {
                throw new ArgumentException("Upload list cannot be empty.", nameof(uploadList));
            }

            foreach (var uploadInfo in uploadList)
            {
                if (string.IsNullOrEmpty(uploadInfo?.FullFilePath))
                {
                    throw new ArgumentException("Full file path cannot be null or empty.", nameof(uploadInfo));
                }

                if (uploadInfo?.TargetUri == null)
                {
                    throw new ArgumentException("Target URI cannot be null.", nameof(uploadInfo));
                }
            }
        }
    }
}
