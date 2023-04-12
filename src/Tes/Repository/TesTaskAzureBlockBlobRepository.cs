using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Tes.Models;

namespace Tes.Repository
{
    public class TesTaskAzureBlockBlobRepository : IRepository<TesTask>
    {
        private readonly BlockBlobDatabase<TesTask> db;
        private readonly ICache<TesTask> cache;
        private readonly ILogger logger;

        public TesTaskAzureBlockBlobRepository(IOptions<BlockBlobDatabaseOptions> options, ILogger<TesTaskAzureBlockBlobRepository> logger, ICache<TesTask> cache = null)
        {
            db = new BlockBlobDatabase<TesTask>(options.Value.StorageAccountName, options.Value.ContainerName, options.Value.ContainerSasToken);
            this.cache = cache;
            this.logger = logger;
            WarmCacheAsync().Wait();
        }

        private async Task WarmCacheAsync()
        {
            if (cache == null)
            {
                logger.LogWarning("Cache is null for TesTaskAzureBlockBlobRepository; no caching will be used.");
                return;
            }

            var sw = Stopwatch.StartNew();
            logger.LogInformation("Warming cache...");

            // Don't allow the state of the system to change until the cache and system are consistent;
            // this is a fast PostgreSQL query even for 1 million items
            await Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(3,
                    retryAttempt =>
                    {
                        logger.LogWarning($"Warming cache retry attempt #{retryAttempt}");
                        return TimeSpan.FromSeconds(10);
                    },
                    (ex, ts) =>
                    {
                        logger.LogCritical(ex, "Couldn't warm cache, is the storage account available?");
                    })
                .ExecuteAsync(async () =>
                {
                    var activeTasks = (await GetActiveItemsAsync()).ToList();
                    var tasksAddedCount = 0;

                    foreach (var task in activeTasks.OrderBy(t => t.CreationTime))
                    {
                        cache?.TryAdd(task.Id, task);
                        tasksAddedCount++;
                    }

                    logger.LogInformation($"Cache warmed successfully in {sw.Elapsed.TotalSeconds:n3} seconds. Added {tasksAddedCount:n0} items to the cache.");
                });
        }

        public async Task<TesTask> CreateItemAsync(TesTask item)
        {
            await db.CreateOrUpdateItemAsync(item.Id, item, item.IsActiveState());
            return item;
        }

        public async Task DeleteItemAsync(string id)
        {
            await db.DeleteItemAsync(id);
        }

        public async Task<IEnumerable<TesTask>> GetItemsAsync(Expression<Func<TesTask, bool>> predicate)
        {
            return await db.GetItemsAsync();
        }
        public async Task<IEnumerable<TesTask>> GetActiveItemsAsync()
        {
            return await db.GetItemsAsync(activeOnly: true);
        }

        public async Task<(string, IEnumerable<TesTask>)> GetItemsAsync(Expression<Func<TesTask, bool>> predicate, int pageSize, string continuationToken)
        {
            // TODO - add support for listing tasks by name
            return await db.GetItemsWithPagingAsync(false, pageSize, continuationToken);
        }

        public async Task<bool> TryGetItemAsync(string id, Action<TesTask> onSuccess = null)
        {
            var item = await db.GetItemAsync(id);
            onSuccess?.Invoke(item);
            return true;
        }

        public async Task<TesTask> UpdateItemAsync(TesTask item)
        {
            await db.CreateOrUpdateItemAsync(item.Id, item, item.IsActiveState());
            return item;
        }

        public void Dispose()
        {

        }
    }
}
