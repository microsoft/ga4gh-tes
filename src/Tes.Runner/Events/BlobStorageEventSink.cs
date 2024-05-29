// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging;
using Tes.Runner.Transfer;
using static System.DateTime;

namespace Tes.Runner.Events
{
    public class BlobStorageEventSink(Uri storageUrl, ILogger<BlobStorageEventSink> logger) : EventSink(logger)
    {
        const string EventTimeStampFormat = "HH-mm-ss.fff";
        // https://learn.microsoft.com/en-us/rest/api/storageservices/version-2023-05-03
        private const string ApiVersion = "2023-05-03";
        private readonly Uri storageUrl = storageUrl ?? throw new ArgumentNullException(nameof(storageUrl));
        private readonly BlobApiHttpUtils blobApiHttpUtils = new(logger);

        public override async Task HandleEventAsync(EventMessage eventMessage)
        {
            try
            {
                var content = JsonSerializer.Serialize(eventMessage, EventMessageContext.Default.EventMessage);

                await blobApiHttpUtils.ExecuteHttpRequestAsync(() =>
                    BlobApiHttpUtils.CreatePutBlobRequestAsync(ToEventUrl(storageUrl, eventMessage), content, ApiVersion, ToTags(eventMessage)));
            }
            catch (Exception e)
            {
                //failure to publish event to blob storage should not fail the execution of the node task
                Logger.LogError(e, "Failed to publish event {EventMessageId} to blob storage", eventMessage.Id);
            }
        }

        private static Uri ToEventUrl(Uri uri, EventMessage message)
        {
            var blobBuilder = new BlobUriBuilder(uri);

            var blobName = ToBlobName(message);

            if (!string.IsNullOrWhiteSpace(blobBuilder.BlobName))
            {
                blobName = $"{blobBuilder.BlobName.TrimEnd('/')}/{blobName}";
            }

            blobBuilder.BlobName = blobName;

            return blobBuilder.ToUri();
        }

        private static Dictionary<string, string> ToTags(EventMessage eventMessage)
        {
            return new Dictionary<string, string>
            {
                { "task-id", eventMessage.EntityId },
                { "workflow-id", eventMessage.CorrelationId },
                { "event-name", eventMessage.Name },
                { "created", eventMessage.Created.ToString(Iso8601DateFormat) }
            };
        }

        private static string ToBlobName(EventMessage eventMessage)
        {
            var blobName =
                $"events/{eventMessage.Name}/{eventMessage.Created.Year}/{eventMessage.Created.Month}/{eventMessage.Created.Day}/{UtcNow.ToString(EventTimeStampFormat)}{eventMessage.Id}.json";
            return blobName;
        }
    }
}
