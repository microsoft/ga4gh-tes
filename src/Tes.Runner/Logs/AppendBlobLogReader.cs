// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Logging;
using Tes.Runner.Transfer;

namespace Tes.Runner.Logs
{
    public class AppendBlobLogReader : StreamLogReader
    {
        //https://learn.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs
        const int MaxNumberOfBlocks = 50000;
        private readonly string targetUrl;
        private readonly BlobApiHttpUtils blobApiHttpUtils = new BlobApiHttpUtils();
        private readonly string stdOutLogName;
        private readonly string stdErrLogName;
        private readonly ILogger logger = PipelineLoggerFactory.Create<AppendBlobLogReader>();

        private int stdOutBLockCount = 0;
        private int stdErrBLockCount = 0;
        private string stdOutBlobName = string.Empty;
        private string stdErrBlobName = string.Empty;


        public AppendBlobLogReader(string targetUrl, string stdOutLogName, string stdErrLogName)
        {
            ArgumentException.ThrowIfNullOrEmpty(targetUrl);
            ArgumentException.ThrowIfNullOrEmpty(stdOutLogName);
            ArgumentException.ThrowIfNullOrEmpty(stdErrLogName);

            this.targetUrl = targetUrl;
            this.stdOutLogName = stdOutLogName;
            this.stdErrLogName = stdErrLogName;
        }

        private string GetBlobNameFromCurrentState(int blockCount, string logName)
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
            var blobNameFromCurrentState = GetBlobNameFromCurrentState(MaxNumberOfBlocks, "stdout");

            if (!blobNameFromCurrentState.Equals(stdOutBlobName, StringComparison.OrdinalIgnoreCase))
            {
                // If we are here is because a new blob is required. Either this is the first call 
                // or the we've reached the max limit of blocks per blob.
                await blobApiHttpUtils.ExecuteHttpRequestAsync(() => BlobApiHttpUtils.CreatePutBlobRequestAsync(
                    targetUrl,
                    data,
                    BlobApiHttpUtils.DefaultApiVersion,
                    default,
                    BlobApiHttpUtils.AppendBlobType));
                return;
            }

            //the blob exists, appending a new block...
            var appendBlockUrl = BlobApiHttpUtils.ParsePutAppendBlockUrl(new Uri(targetUrl));

            await blobApiHttpUtils.ExecuteHttpRequestAsync(() => BlobApiHttpUtils.CreatePutAppendBlockRequestAsync(
                data,
                appendBlockUrl.ToString(),
                BlobApiHttpUtils.DefaultApiVersion));

            //increment block count state
            stdOutBLockCount++;
        }

        public override Task AppendStandardErrAsync(string data)
        {
            throw new NotImplementedException();
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
