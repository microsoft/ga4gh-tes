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
        /// <summary>
        /// Configures each part with the put block URL.
        /// </summary>
        /// <param name="buffer">Pipeline buffer to configure</param>
        public override void ConfigurePipelineBuffer(PipelineBuffer buffer)
        {
            buffer.BlobPartUrl =
                BlobBlockApiHttpUtils.ParsePutBlockUrl(buffer.BlobPartUrl, buffer.Ordinal);
        }
        /// <summary>
        /// Writes the part as a block to the blob.
        /// </summary>
        /// <param name="buffer">Pipeline buffer containg the block data</param>
        /// <returns>Number of bytes written</returns>
        public override async ValueTask<int> ExecuteWriteAsync(PipelineBuffer buffer)
        {
            var request = BlobBlockApiHttpUtils.CreatePutBlockRequestAsync(buffer, PipelineOptions.ApiVersion);

            await ExecuteHttpRequestAsync(request);

            return buffer.Length;
        }
        /// <summary>
        /// Reads part's data from the file
        /// </summary>
        /// <param name="buffer">Pipeline buffer in which the file data will be written</param>
        /// <returns>Number of bytes read</returns>
        public override async ValueTask<int> ExecuteReadAsync(PipelineBuffer buffer)
        {
            var fileHandler = await buffer.FileHandlerPool.Reader.ReadAsync();

            fileHandler.Position = buffer.Offset;

            var dataRead = await fileHandler.ReadAsync(buffer.Data, 0, buffer.Length);

            await buffer.FileHandlerPool.Writer.WriteAsync(fileHandler);

            return dataRead;
        }
        /// <summary>
        /// Reads the source file length
        /// </summary>
        /// <param name="lengthSource">File path</param>
        /// <returns>File size in number of bytes</returns>
        public override Task<long> GetSourceLengthAsync(string lengthSource)
        {
            return Task.FromResult((new FileInfo(lengthSource)).Length);
        }

        /// <summary>
        /// Creates the blob block list and commits the blob.
        /// </summary>
        /// <param name="length">Blob size</param>
        /// <param name="blobUrl">Target Blob URL</param>
        /// <param name="fileName">Source file name</param>
        /// <returns></returns>
        public override async Task OnCompletionAsync(long length, Uri? blobUrl, string fileName)
        {
            ArgumentNullException.ThrowIfNull(blobUrl, nameof(blobUrl));
            ArgumentException.ThrowIfNullOrEmpty(fileName, nameof(fileName));

            var request = BlobBlockApiHttpUtils.CreateBlobBlockListRequest(length, blobUrl, PipelineOptions.BlockSize, PipelineOptions.ApiVersion);

            await ExecuteHttpRequestAsync(request);
        }

        private async ValueTask ExecuteHttpRequestAsync(HttpRequestMessage request)
        {
            var response = await HttpClient.SendAsync(request);

            response.EnsureSuccessStatusCode();
        }
        /// <summary>
        /// Performs a batch upload of the files to the specified target URIs.
        /// The URIs are Azure Block Blob URIs with SAS tokens.
        /// </summary>
        /// <param name="uploadList">File upload list.</param>
        /// <returns></returns>
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
