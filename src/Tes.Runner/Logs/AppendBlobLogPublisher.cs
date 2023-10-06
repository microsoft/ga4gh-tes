// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging;
using Tes.Runner.Transfer;

namespace Tes.Runner.Logs
{
    public class AppendBlobLogPublisher : StreamLogReader
    {
        //https://learn.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs
        const int MaxNumberOfBlocks = 50000;
        private readonly string targetUrl;
        private readonly BlobApiHttpUtils blobApiHttpUtils = new BlobApiHttpUtils();
        private readonly string stdOutLogNamePrefix;
        private readonly string stdErrLogNamePrefix;
        private readonly ILogger logger = PipelineLoggerFactory.Create<AppendBlobLogPublisher>();

        private int currentStdOutBLockCount;
        private int currentStdErrBLockCount;

        private string currentStdOutBlobName = string.Empty;
        private string currentStdErrBlobName = string.Empty;


        public AppendBlobLogPublisher(string targetUrl, string stdOutLogNamePrefix, string stdErrLogNamePrefix)
        {
            ArgumentException.ThrowIfNullOrEmpty(targetUrl);
            ArgumentException.ThrowIfNullOrEmpty(stdOutLogNamePrefix);
            ArgumentException.ThrowIfNullOrEmpty(stdErrLogNamePrefix);

            this.targetUrl = targetUrl;
            this.stdOutLogNamePrefix = stdOutLogNamePrefix;
            this.stdErrLogNamePrefix = stdErrLogNamePrefix;
        }

        private string GetBlobNameConsideringBlockCountCurrentState(int blockCount, string logName)
        {
            var blobNumber = blockCount / MaxNumberOfBlocks;

            if (blobNumber == 0)
            {
                return $"{logName}";
            }

            return $"{logName}_{blobNumber}";
        }



        public override async Task AppendStandardOutputAsync(string data)
        {
            currentStdOutBlobName = await CreateOrAppendLogDataAsync(data, currentStdOutBLockCount, currentStdOutBlobName, stdOutLogNamePrefix);

            //increment block count state
            currentStdOutBLockCount++;
        }

        private async Task<string> CreateOrAppendLogDataAsync(string data, int currentBlockCount, string currentBlobLogName, string baseLogName)
        {
            var targetUri = GetUriAndBlobNameFromCurrentState(currentBlockCount, baseLogName, out var blobNameFromCurrentState);

            if (!blobNameFromCurrentState.Equals(currentBlobLogName, StringComparison.OrdinalIgnoreCase))
            {
                // If we are here is because a new blob is required. Either this is the first call 
                // or the we've reached the max limit of blocks per blob.
                await blobApiHttpUtils.ExecuteHttpRequestAsync(() => BlobApiHttpUtils.CreatePutBlobRequestAsync(
                    targetUri,
                    content: default,  //append blobs are created empty
                    BlobApiHttpUtils.DefaultApiVersion,
                    tags: default,
                    BlobApiHttpUtils.AppendBlobType));
            }

            //Appending a new block...
            await blobApiHttpUtils.ExecuteHttpRequestAsync(() => BlobApiHttpUtils.CreatePutAppendBlockRequestAsync(
                data,
                targetUri,
                BlobApiHttpUtils.DefaultApiVersion));

            return blobNameFromCurrentState;
        }

        public Uri GetUriAndBlobNameFromCurrentState(int currentBlockCount, string baseLogName,
            out string blobNameFromCurrentState)
        {
            blobNameFromCurrentState = GetBlobNameConsideringBlockCountCurrentState(currentBlockCount, baseLogName);

            var blobBuilder = new BlobUriBuilder(new Uri(targetUrl));

            var blobName = blobNameFromCurrentState;

            if (!string.IsNullOrWhiteSpace(blobBuilder.BlobName))
            {
                blobName = $"{blobBuilder.BlobName.TrimEnd('/')}/{blobName}";
            }

            blobBuilder.BlobName = blobName;

            var targetUri = blobBuilder.ToUri();
            return targetUri;
        }

        public override async Task AppendStandardErrAsync(string data)
        {
            currentStdErrBlobName = await CreateOrAppendLogDataAsync(data, currentStdErrBLockCount, currentStdErrBlobName, stdErrLogNamePrefix);

            //increment block count state
            currentStdErrBLockCount++;
        }

        public override void OnComplete(Exception? err)
        {
            if (err is not null)
            {
                logger.LogError(err, "Failed to stream logs to blob storage");
            }
        }
    }
}
