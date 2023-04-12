// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Storage.Blobs;
using Polly;

namespace Tes.Repository
{
    public class BlockBlobDatabase<T> where T : class
    {
        private const int maxConcurrentItemDownloads = 64;
        private readonly BlobServiceClient blobServiceClient;
        private readonly BlobContainerClient container;
        private const string activeStatePrefix = "a/";
        private const string inactiveStatePrefix = "z/";
        private string GetActiveBlobNameById(string id) => $"{activeStatePrefix}{id}.json";
        private string GetInactiveBlobNameById(string id) => $"{inactiveStatePrefix}{id}.json";
        public string StorageAccountName { get; set; }
        public string ContainerName { get; set; } 

        public BlockBlobDatabase(string storageAccountName, string containerName, string containerSasToken = null)
        {
            StorageAccountName = storageAccountName;
            ContainerName = containerName;

            if (!string.IsNullOrWhiteSpace(containerSasToken))
            {
                blobServiceClient = new BlobServiceClient(new Uri($"https://{StorageAccountName}.blob.core.windows.net?{containerSasToken.TrimStart('?')}"));
            }
            else
            {
                blobServiceClient = new BlobServiceClient(new Uri($"https://{StorageAccountName}.blob.core.windows.net"), new DefaultAzureCredential());
            }

            container = blobServiceClient.GetBlobContainerClient(ContainerName);
            container.CreateIfNotExistsAsync().Wait();
        }

        public async Task CreateOrUpdateItemAsync(string id, T item, bool isActive)
        {
            var json = System.Text.Json.JsonSerializer.Serialize(item);

            if (!isActive)
            {
                // Delete active if it exists
                var blobClient1 = container.GetBlobClient(GetActiveBlobNameById(id));
                var task1 = blobClient1.DeleteIfExistsAsync();

                // Update/create active
                var blobClient2 = container.GetBlobClient(GetInactiveBlobNameById(id));
                var task2 = blobClient2.UploadAsync(BinaryData.FromString(json), overwrite: true);

                // Retry to reduce likelihood of one blob succeeding and the other failing
                await Policy
                    .Handle<Exception>()
                    .WaitAndRetryAsync(10, retryAttempt => TimeSpan.FromSeconds(1))
                    .ExecuteAsync(async () => await Task.WhenAll(task1, task2));
            }
            else
            {
                // Assumption: a task can never go from inactive to active, so no need to delete anything here
                var blobClient = container.GetBlobClient($"{id}.json");
                var activeBlobTask = await blobClient.UploadAsync(BinaryData.FromString(json), overwrite: true);
            }
        }

        public async Task DeleteItemAsync(string id)
        {
            var blobClient = container.GetBlobClient(GetActiveBlobNameById(id));
            var blobClient2 = container.GetBlobClient(GetInactiveBlobNameById(id));
            var task1 = blobClient.DeleteIfExistsAsync();
            var task2 = blobClient2.DeleteIfExistsAsync();

            // Retry to reduce likelihood of one blob succeeding and the other failing
            await Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(10, retryAttempt => TimeSpan.FromSeconds(1))
                .ExecuteAsync(async () => await Task.WhenAll(task1, task2));
        }

        public async Task<T> GetItemAsync(string id)
        {
            // Check if inactive exists first, since inactive will never go to active state, to make more consistent results
            var blobClient = container.GetBlobClient(GetInactiveBlobNameById(id));

            if (await blobClient.ExistsAsync())
            {
                var inactiveBlobJson = (await blobClient.DownloadContentAsync()).Value.Content.ToString();
                return System.Text.Json.JsonSerializer.Deserialize<T>(inactiveBlobJson);
            }

            blobClient = container.GetBlobClient(GetActiveBlobNameById(id));
            var json = (await blobClient.DownloadContentAsync()).Value.Content.ToString();
            return System.Text.Json.JsonSerializer.Deserialize<T>(json);
        }

        /// <summary>
        /// Downloads all items in parallel
        /// Specifically designed NOT to enumerate items to prevent caller stalling the download throughput
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public async Task<IList<T>> GetItemsAsync(bool activeOnly = false)
        {
            var enumerator = container.GetBlobsAsync(prefix: activeOnly ? activeStatePrefix : null).GetAsyncEnumerator();
            var blobNames = new List<string>();

            while (await enumerator.MoveNextAsync())
            {
                // example: a/0fb0858a-3166-4a22-85b6-4337df2f53c5.json
                // example: z/0fb0858a-3166-4a22-85b6-4337df2f53c5.json
                var blobName = enumerator.Current.Name;
                blobNames.Add(blobName);
            }

            return await DownloadBlobsAsync(blobNames);
        }

        public async Task<(string, IList<T>)> GetItemsWithPagingAsync(bool activeOnly = false, int pageSize = 5000, string continuationToken = null)
        {
            var blobNames = new List<string>();

            while (true)
            {
                var pages = container.GetBlobsAsync(prefix: activeOnly ? activeStatePrefix : null).AsPages(continuationToken, pageSize);
                var enumerator = pages.GetAsyncEnumerator();
                var isMoreItems = await enumerator.MoveNextAsync();

                if (!isMoreItems)
                {
                    return (null, new List<T>());
                }

                var page = enumerator.Current;

                foreach (var blob in page.Values)
                {
                    blobNames.Add(blob.Name);
                }
                    
                return (page.ContinuationToken, await DownloadBlobsAsync(blobNames));
            }
        }

        private async Task<IList<T>> DownloadBlobsAsync(List<string> blobNames)
        {
            var downloadQueue = new ConcurrentQueue<string>(blobNames);
            var items = new ConcurrentBag<T>();
            long runningTasksCount = 0;

            while (downloadQueue.TryDequeue(out var blobName))
            {
                while (Interlocked.Read(ref runningTasksCount) >= maxConcurrentItemDownloads)
                {
                    // Pause while maxed out
                    await Task.Delay(50);
                }

                Interlocked.Increment(ref runningTasksCount);

                _ = Task.Run(async () =>
                {
                    try
                    {
                        var blobClient = container.GetBlobClient(blobName);
                        var json = (await blobClient.DownloadContentAsync()).Value.Content.ToString();
                        items.Add(System.Text.Json.JsonSerializer.Deserialize<T>(json));
                    }
                    catch (Exception exc)
                    {
                        // TODO log?
                        downloadQueue.Enqueue(blobName);
                    }

                    Interlocked.Decrement(ref runningTasksCount);
                });
            }

            while (Interlocked.Read(ref runningTasksCount) > 0)
            {
                // Wait for all downloads to complete
                await Task.Delay(50);
            }

            return items.ToList();
        }
    }
}
