// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging;
using Tes.Runner.Transfer;
using static System.DateTime;

namespace Tes.Runner.Events
{
    public class BlobStorageEventSink : EventSink
    {
        const string EventTimeStampFormat = "HH-mm-ss.fff";
        // https://learn.microsoft.com/en-us/rest/api/storageservices/version-2023-05-03
        private const string ApiVersion = "2023-05-03";
        private readonly Uri storageUrl;
        private readonly ILogger logger = PipelineLoggerFactory.Create<BlobStorageEventSink>();
        private readonly BlobBlockApiHttpUtils blobBlockApiHttpUtils = new BlobBlockApiHttpUtils();


        public BlobStorageEventSink(Uri storageUrl)
        {
            ArgumentNullException.ThrowIfNull(storageUrl);

            this.storageUrl = storageUrl;
        }

        public override async Task HandleEventAsync(EventMessage eventMessage)
        {
            try
            {
                var content = JsonSerializer.Serialize(eventMessage);

                await blobBlockApiHttpUtils.ExecuteHttpRequestAsync(() =>
                    BlobBlockApiHttpUtils.CreatePutBlobRequestAsync(ToEventUrl(storageUrl, eventMessage), content, ApiVersion, ToEventTag(eventMessage)));
            }
            catch (Exception e)
            {
                //failure to publish event to blob storage should not fail the execution of the node task
                logger.LogError(e, $"Failed to publish event {eventMessage.Id} to blob storage");
            }
        }

        private string ToEventUrl(Uri uri, EventMessage message)
        {
            var blobBuilder = new BlobUriBuilder(uri);

            var blobName = ToBlobName(message);

            if (!string.IsNullOrWhiteSpace(blobBuilder.BlobName))
            {
                blobName = $"{blobBuilder.BlobName.TrimEnd('/')}/{blobName}";
            }

            blobBuilder.BlobName = blobName;

            return blobBuilder.ToUri().ToString();
        }

        private static string ToBlobName(EventMessage eventMessage)
        {
            var blobName =
                $"events/{eventMessage.Name}/{eventMessage.Created.Year}/{eventMessage.Created.Month}/{eventMessage.Created.Day}/{UtcNow.ToString(EventTimeStampFormat)}{eventMessage.Id}.json";
            return blobName;
        }
    }
}
