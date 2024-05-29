// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging;
using Tes.Runner.Transfer;

namespace Tes.Runner.Logs
{
    /// <summary>
    /// Reads the log data from the stream and incrementally publishes it to Azure storage as append blobs.
    /// </summary>
    public class AppendBlobLogPublisher : StreamLogReader
    {
        private readonly Uri targetUrl;
        private readonly BlobApiHttpUtils blobApiHttpUtils;
        private readonly string stdOutLogNamePrefix;
        private readonly string stdErrLogNamePrefix;

        private int currentStdOutBlockCount;
        private int currentStdErrBlockCount;

        private string currentStdOutBlobName = string.Empty;
        private string currentStdErrBlobName = string.Empty;


        public AppendBlobLogPublisher(Uri targetUrl, string logNamePrefix, ILogger<AppendBlobLogPublisher> logger)
            : base(logger)
        {
            ArgumentNullException.ThrowIfNull(targetUrl);
            ArgumentException.ThrowIfNullOrEmpty(logNamePrefix);

            this.targetUrl = targetUrl;
            var prefixTimeStamp = DateTime.UtcNow.ToString("yyyyMMddHHmmssfff");
            stdOutLogNamePrefix = $"{logNamePrefix}_stdout_{prefixTimeStamp}";
            stdErrLogNamePrefix = $"{logNamePrefix}_stderr_{prefixTimeStamp}";
            blobApiHttpUtils = new(logger);
        }

        private static string GetBlobNameConsideringBlockCountCurrentState(int blockCount, string logName)
        {
            var blobNumber = blockCount / BlobSizeUtils.MaxBlobBlocksCount;

            if (blobNumber == 0)
            {
                return $"{logName}.txt";
            }

            return $"{logName}_{blobNumber}.txt";
        }

        public override async Task AppendStandardOutputAsync(string data)
        {
            currentStdOutBlobName = await CreateOrAppendLogDataAsync(data, currentStdOutBlockCount, currentStdOutBlobName, stdOutLogNamePrefix);

            //increment block count state
            currentStdOutBlockCount++;
        }

        private async Task<string> CreateOrAppendLogDataAsync(string data, int currentBlockCount, string currentBlobLogName, string baseLogName)
        {
            var targetUri = GetUriAndBlobNameFromCurrentState(currentBlockCount, baseLogName, out var blobNameFromCurrentState);

            if (!blobNameFromCurrentState.Equals(currentBlobLogName, StringComparison.OrdinalIgnoreCase))
            {
                // A new blob is required - a new name was returned.
                // Either this is the first call or it's reached the max limit of blocks per blob.
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

            var blobBuilder = new BlobUriBuilder(targetUrl);

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
            currentStdErrBlobName = await CreateOrAppendLogDataAsync(data, currentStdErrBlockCount, currentStdErrBlobName, stdErrLogNamePrefix);

            //increment block count state
            currentStdErrBlockCount++;
        }

        public override void OnComplete(Exception? err)
        {
            if (err is not null)
            {
                Logger.LogError(err, "Failed to stream logs to blob storage");
            }
        }
    }
}
