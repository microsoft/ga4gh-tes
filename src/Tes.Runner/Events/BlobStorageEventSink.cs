// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text;
using System.Text.Json;
using Azure.Storage.Blobs;

namespace Tes.Runner.Events
{
    public class BlobStorageEventSink : EventSink
    {
        private readonly Uri storageUrl;

        public BlobStorageEventSink(Uri storageUrl)
        {
            this.storageUrl = storageUrl;
        }

        public override async Task HandleEventAsync(EventMessage eventMessage)
        {
            var blobClient = GetBlobClient(eventMessage);

            var json = JsonSerializer.Serialize(eventMessage);

            await blobClient.UploadAsync(new MemoryStream(Encoding.UTF8.GetBytes(json)), overwrite: true);

            await blobClient.SetTagsAsync(ToEventTag(eventMessage));
        }

        private BlobClient GetBlobClient(EventMessage eventMessage)
        {
            var blobName = ToBlobName(eventMessage);

            var blobUrl = new BlobUriBuilder(storageUrl) { BlobName = blobName };

            var blobClient = new BlobClient(blobUrl.ToUri());
            return blobClient;
        }

        private static string ToBlobName(EventMessage eventMessage)
        {
            var blobName = $"{eventMessage.Name}/{eventMessage.Created.Year}/{eventMessage.Created.Month}/{eventMessage.Created.Day}/{eventMessage.Created.Hour}/{eventMessage.Id}.json";
            return blobName;
        }
    }
}
