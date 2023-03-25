using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection.Metadata;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Polly;
using Tes.Models;

namespace Tes.Repository
{
    public class BlockBlobDatabase
    {
        private const int maxConcurrentItemDownloads = 64;
        private const string WorkflowsContainerName = "workflows";
        private readonly CloudStorageAccount account;
        private readonly CloudBlobClient blobClient;
        private readonly HashSet<string> createdContainers = new();

        public Task CreateItemAsync<T>(string id, T item, string initialState)
        {
            var containerReference = blobClient.GetContainerReference(container);

            if (!createdContainers.Contains(container.ToLowerInvariant()))
            {
                // Only attempt to create the container once per lifetime of the process
                await containerReference.CreateIfNotExistsAsync();
                createdContainers.Add(container.ToLowerInvariant());
            }

            var blob = containerReference.GetBlockBlobReference($"{initialState}/{id}.json");
            var json = System.Text.Json.JsonSerializer.Serialize(item);
            await blob.UploadTextAsync(json);
        }

        public Task DeleteItemAsync(string id)
        {
            blobClient.GetContainerReference(container).GetBlockBlobReference(blobName).DeleteIfExistsAsync();
        }

        public async Task<List<T>> GetItemsByStateAsync<T>(T state) where T : Enum
        {
            var containerReference = blobClient.GetContainerReference(WorkflowsContainerName);
            var lowercaseState = state.ToString().ToLowerInvariant();

            // ListBlobs by prefix (fastest Azure Block Blob list)
            var blobs = await Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(3,
                    retryAttempt =>
                    {
                        return TimeSpan.FromSeconds(1);
                    },
                    (ex, ts) =>
                    {
                        // todo log
                    })
                .ExecuteAsync<CloudBlockBlob>(GetBlobsWithPrefixAsync(containerReference, lowercaseState));

            var readmeBlobName = $"{lowercaseState}/readme.txt";

            // Force materialization
            return await DownloadInParallelAsync(blobs);

            static async Task<IEnumerable<T>> DownloadInParallelAsync(ConcurrentQueue<CloudBlockBlob> queue)
            {
                var blobQueue = new ConcurrentQueue<CloudBlockBlob>(blobs
                    .Where(blob => !blob.Name.Equals(lowercaseState, StringComparison.OrdinalIgnoreCase))
                    .Where(blob => !blob.Name.Equals(readmeBlobName, StringComparison.OrdinalIgnoreCase))
                    .Where(blob => blob.Properties.LastModified.HasValue)
                    .Select(b => b));

                var items = new ConcurrentBag<T>();
                long taskCount = 0;

                while (queue.TryDequeue(out var blob))
                {
                    while (Interlocked.Read(ref taskCount) >= maxConcurrentItemDownloads)
                    {
                        // Pause while maxed out
                        await Task.Delay(20);
                    }

                    Interlocked.Increment(ref taskCount);

                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            items.Add(await DownloadAndDeserializeItemAsync<T>(blob));
                        }
                        catch (Exception)
                        {
                            queue.Enqueue(blob);
                        }

                        Interlocked.Decrement(ref taskCount);
                    });
                }

                while (Interlocked.Read(ref taskCount) > 0)
                {
                    // Wait for all downloads to complete
                    await Task.Delay(20);
                }

                return items;

                static async Task<T> DownloadAndDeserializeItemAsync<T>(CloudBlockBlob blob)
                {
                    var json = await blob.DownloadTextAsync();
                    return System.Text.Json.JsonSerializer.Deserialize<T>(json);
                }
            }
        }

        private static async Task<IEnumerable<CloudBlockBlob>> GetBlobsWithPrefixAsync(CloudBlobContainer blobContainer, string prefix)
        {
            var blobList = new List<CloudBlockBlob>();

            BlobContinuationToken continuationToken = null;

            do
            {
                var partialResult = await blobContainer.ListBlobsSegmentedAsync(
                    prefix: prefix,
                    useFlatBlobListing: true,
                    currentToken: continuationToken,
                    blobListingDetails: BlobListingDetails.None,
                    maxResults: null,
                    options: null,
                    operationContext: null);

                continuationToken = partialResult.ContinuationToken;

                blobList.AddRange(partialResult.Results.OfType<CloudBlockBlob>());
            }
            while (continuationToken is not null);

            return blobList;
        }
    }
}
