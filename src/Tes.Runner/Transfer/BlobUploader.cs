﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Tes.Runner.Transfer
{
    /// <summary>
    /// Blob operation pipeline for uploading blobs.
    /// </summary>
    public class BlobUploader : BlobOperationPipeline
    {
        private readonly ConcurrentDictionary<string, Md5HashListProvider> hashListProviders = new();

        public BlobUploader(BlobPipelineOptions pipelineOptions, Channel<byte[]> memoryBufferPool) : base(pipelineOptions, memoryBufferPool)
        {
        }

        /// <summary>
        /// Parameter-less constructor for mocking
        /// </summary>
        protected BlobUploader() : base(new BlobPipelineOptions(), Channel.CreateUnbounded<byte[]>())
        {
        }

        /// <summary>
        /// Configures each part with the put block URL.
        /// </summary>
        /// <param name="buffer">Pipeline buffer to configure</param>
        public override void ConfigurePipelineBuffer(PipelineBuffer buffer)
        {
            buffer.BlobPartUrl = BlobApiHttpUtils.ParsePutBlockUrl(buffer.BlobUrl, buffer.Ordinal);

            buffer.HashListProvider = hashListProviders.GetOrAdd(buffer.FileName, new Md5HashListProvider());
        }

        /// <summary>
        /// Writes the part as a block to the blob.
        /// </summary>
        /// <param name="buffer">Pipeline buffer containing the block data</param>
        /// <param name="cancellationToken"></param>
        /// <returns>Number of bytes written</returns>
        public override async ValueTask<int> ExecuteWriteAsync(PipelineBuffer buffer, CancellationToken cancellationToken)
        {
            if (IsFileEmptyScenario(buffer))
            {
                return 0;
            }

            HttpResponseMessage? response = null;

            try
            {
                response = await BlobApiHttpUtils.ExecuteHttpRequestAsync(() => BlobApiHttpUtils.CreatePutBlockRequestAsync(buffer, PipelineOptions.ApiVersion), cancellationToken);
            }
            finally
            {
                response?.Dispose();
            }

            return buffer.Length;
        }

        private static bool IsFileEmptyScenario(PipelineBuffer buffer)
        {
            if (buffer.Length == 0)
            {
                if (buffer.NumberOfParts == 1)
                {
                    // If there is only one part and it is empty, no need to write an empty block to the blob.
                    {
                        return true;
                    }
                }

                throw new Exception(
                    "Invalid operation. The buffer is empty and the transfer contains more than one part");
            }

            return false;
        }

        /// <summary>
        /// Reads part's data from the file
        /// </summary>
        /// <param name="buffer">Pipeline buffer in which the file data will be written</param>
        /// <param name="cancellationToken">Signals cancellation of read operations on the channels and file handler</param>
        /// <returns>Number of bytes read</returns>
        public override async ValueTask<int> ExecuteReadAsync(PipelineBuffer buffer, CancellationToken cancellationToken)
        {
            var fileHandler = await buffer.FileHandlerPool.Reader.ReadAsync(cancellationToken);

            fileHandler.Position = buffer.Offset;

            var dataRead = await fileHandler.ReadAsync(buffer.Data, 0, buffer.Length, cancellationToken);

            buffer.HashListProvider?.CalculateAndAddBlockHash(buffer);

            await buffer.FileHandlerPool.Writer.WriteAsync(fileHandler, cancellationToken);

            return dataRead;
        }


        /// <summary>
        /// Reads the source file length
        /// </summary>
        /// <param name="lengthSource">File path</param>
        /// <returns>File size in number of bytes</returns>
        public override Task<long> GetSourceLengthAsync(string lengthSource)
        {
            return Task.FromResult(new FileInfo(lengthSource).Length);
        }

        /// <summary>
        /// Creates the blob block list and commits the blob.
        /// </summary>
        /// <param name="length">Blob size</param>
        /// <param name="blobUrl">Target Blob URL</param>
        /// <param name="fileName">Source file name</param>
        /// <param name="rootHash">Root hash of the file</param>
        /// <param name="contentMd5">Content MD5 hash</param>
        /// <returns></returns>
        public override async Task OnCompletionAsync(long length, Uri? blobUrl, string fileName, string? rootHash, string? contentMd5)
        {
            ArgumentNullException.ThrowIfNull(blobUrl, nameof(blobUrl));
            ArgumentException.ThrowIfNullOrEmpty(fileName, nameof(fileName));

            HttpResponseMessage? response = null;
            try
            {
                response = await BlobApiHttpUtils.ExecuteHttpRequestAsync(() =>
                    BlobApiHttpUtils.CreateBlobBlockListRequest(length, blobUrl, PipelineOptions.BlockSizeBytes,
                        PipelineOptions.ApiVersion, rootHash, contentMd5));
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Failed to complete the blob block operation");
                throw;
            }
            finally
            {
                response?.Dispose();
            }
        }

        /// <summary>
        /// Performs a batch upload of the files to the specified target URIs.
        /// The URIs are Azure Block Blob URIs with SAS tokens.
        /// </summary>
        /// <param name="uploadList">File upload list.</param>
        /// <returns></returns>
        public virtual async Task<long> UploadAsync(List<UploadInfo> uploadList)
        {
            ValidateUploadList(uploadList);

            var operationList = uploadList.Select(d => new BlobOperationInfo(d.TargetUri, d.FullFilePath, d.FullFilePath, true)).ToList();

            return await ExecutePipelineAsync(operationList);
        }

        private static void ValidateUploadList(List<UploadInfo> uploadList)
        {
            ArgumentNullException.ThrowIfNull(uploadList, nameof(uploadList));

            if (uploadList.Count == 0)
            {
                throw new ArgumentException("Upload list cannot be empty.", nameof(uploadList));
            }

            foreach (var uploadInfo in uploadList)
            {
                if (string.IsNullOrEmpty(uploadInfo.FullFilePath))
                {
                    throw new ArgumentException("Full file path cannot be null or empty.");
                }

                if (uploadInfo.TargetUri == null)
                {
                    throw new ArgumentException("Target URI cannot be null.");
                }
            }
        }
    }
}
