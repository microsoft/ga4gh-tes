// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Concurrent;
using System.Threading.Channels;
using Docker.DotNet.Models;
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
        /// Configures each part with the put block URL.
        /// </summary>
        /// <param name="buffer">Pipeline buffer to configure</param>
        public override void ConfigurePipelineBuffer(PipelineBuffer buffer)
        {
            buffer.BlobPartUrl = BlobBlockApiHttpUtils.ParsePutBlockUrl(buffer.BlobUrl, buffer.Ordinal);

            buffer.HashListProvider = hashListProviders.GetOrAdd(buffer.FileName, new Md5HashListProvider());
        }

        /// <summary>
        /// Writes the part as a block to the blob.
        /// </summary>
        /// <param name="buffer">Pipeline buffer containing the block data</param>
        /// <returns>Number of bytes written</returns>
        public override async ValueTask<int> ExecuteWriteAsync(PipelineBuffer buffer)
        {
            if (IsFileEmptyScenario(buffer))
            {
                return 0;
            }

            await BlobBlockApiHttpUtils.ExecuteHttpRequestAsync(() => BlobBlockApiHttpUtils.CreatePutBlockRequestAsync(buffer, PipelineOptions.ApiVersion));

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
        /// <returns>Number of bytes read</returns>
        public override async ValueTask<int> ExecuteReadAsync(PipelineBuffer buffer)
        {
            var fileHandler = await buffer.FileHandlerPool.Reader.ReadAsync();

            fileHandler.Position = buffer.Offset;

            var dataRead = await fileHandler.ReadAsync(buffer.Data, 0, buffer.Length);

            buffer.HashListProvider?.CalculateAndAddBlockHash(buffer);

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
            return Task.FromResult(new FileInfo(lengthSource).Length);
        }

        /// <summary>
        /// Creates the blob block list and commits the blob.
        /// </summary>
        /// <param name="length">Blob size</param>
        /// <param name="blobUrl">Target Blob URL</param>
        /// <param name="fileName">Source file name</param>
        /// <param name="rootHash">Root hash of the file</param>
        /// <returns></returns>
        public override async Task OnCompletionAsync(long length, Uri? blobUrl, string fileName, string? rootHash)
        {
            ArgumentNullException.ThrowIfNull(blobUrl, nameof(blobUrl));
            ArgumentException.ThrowIfNullOrEmpty(fileName, nameof(fileName));

            try
            {
                await BlobBlockApiHttpUtils.ExecuteHttpRequestAsync(() => BlobBlockApiHttpUtils.CreateBlobBlockListRequest(length, blobUrl, PipelineOptions.BlockSizeBytes, PipelineOptions.ApiVersion, rootHash));
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Failed to complete the blob block operation");
                throw;
            }
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

            //var fileMd5ProcessorTasks = GetFileBlakeProcessorTasks(uploadList);

            var operationList = uploadList.Select(d => new BlobOperationInfo(d.TargetUri, d.FullFilePath, d.FullFilePath, true)).ToList();

            var result = await ExecutePipelineAsync(operationList);

            //Task.WaitAll(fileMd5ProcessorTasks.ToArray());

            return result;
        }

        private List<Task<string>> GetFileMd5ProcessorTasks(List<UploadInfo> uploadList)
        {
            return uploadList.Select(upload => FileHashProcessor.StartNewMd5Processor(upload.FullFilePath).GetFileMd5HashAsync()).ToList();
        }
        private List<Task<string>> GetFileBlakeProcessorTasks(List<UploadInfo> uploadList)
        {
            return uploadList.Select(upload => FileHashProcessor.StartNewBlake5Processor(upload.FullFilePath).GetFileMd5HashAsync()).ToList();
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
