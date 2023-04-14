// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Repository
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Options;
    using Polly;
    using Tes.Models;
    using Tes.Utilities;

    /// <summary>
    /// A TesTask specific repository for storing the TesTask as JSON within an Entity Framework Postgres table
    /// </summary>
    /// <typeparam name="TesTask"></typeparam>
    /// <remarks>
    /// This repository batches updates, as that is expected to be the most used "write"-type action.
    /// </remarks>
    public sealed class TesTaskPostgreSqlRepository : IRepository<TesTask>
    {
        private readonly Func<TesDbContext> createDbContext;
        private readonly ICache<TesTask> cache;
        private readonly ILogger logger;
        private readonly BackgroundWorker updaterWorker = new();
        private readonly ConcurrentQueue<(TesTask TesTask, TaskCompletionSource<TesTask> TaskSource)> tasksToUpdate = new();

        /// <summary>
        /// Default constructor that also will create the schema if it does not exist
        /// </summary>
        /// <param name="connectionString">The PostgreSql connection string</param>
        public TesTaskPostgreSqlRepository(IOptions<PostgreSqlOptions> options, ILogger<TesTaskPostgreSqlRepository> logger, ICache<TesTask> cache = null)
        {
            this.cache = cache;
            this.logger = logger;
            updaterWorker.WorkerSupportsCancellation = true;
            updaterWorker.DoWork += UpdaterWorkerProc;
            var connectionString = new ConnectionStringUtility().GetPostgresConnectionString(options);
            createDbContext = () => { return new TesDbContext(connectionString); };
            using var dbContext = createDbContext();
            dbContext.Database.MigrateAsync().Wait();
            WarmCacheAsync().Wait();
            updaterWorker.RunWorkerAsync();
        }

        /// <summary>
        /// Constructor for testing to enable mocking DbContext 
        /// </summary>
        /// <param name="createDbContext">A delegate that creates a TesTaskPostgreSqlRepository context</param>
        public TesTaskPostgreSqlRepository(Func<TesDbContext> createDbContext)
        {
            updaterWorker.WorkerSupportsCancellation = true;
            updaterWorker.DoWork += UpdaterWorkerProc;
            this.createDbContext = createDbContext;
            using var dbContext = createDbContext();
            dbContext.Database.MigrateAsync().Wait();
            updaterWorker.RunWorkerAsync();
        }

        private async Task WarmCacheAsync()
        {
            if (cache == null)
            {
                logger.LogWarning("Cache is null for TesTaskPostgreSqlRepository; no caching will be used.");
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
                        logger.LogCritical(ex, "Couldn't warm cache, is the database online?");
                    })
                .ExecuteAsync(async () =>
                {
                    var activeTasks = await GetItemsAsync(task => TesTask.ActiveStates.Contains(task.State), CancellationToken.None);
                    var tasksAddedCount = 0;

                    foreach (var task in activeTasks.OrderBy(t => t.CreationTime))
                    {
                        cache?.TryAdd(task.Id, task);
                        tasksAddedCount++;
                    }

                    logger.LogInformation($"Cache warmed successfully in {sw.Elapsed.TotalSeconds:n3} seconds. Added {tasksAddedCount:n0} items to the cache.");
                });
        }


        /// <inheritdoc/>
        public async Task<bool> TryGetItemAsync(string id, Action<TesTask> onSuccess, CancellationToken cancellationToken)
        {
            if (cache?.TryGetValue(id, out var task) == true)
            {
                onSuccess?.Invoke(task);

                if (!task.IsActiveState())
                {
                    // Cache optimization because we can assume that most of the time, the workflow engine will no longer "GET" after a terminal state
                    cache?.TryRemove(task.Id);
                }

                return true;
            }

            using var dbContext = createDbContext();

            // Search for Id within the JSON
            var item = await dbContext.TesTasks.FirstOrDefaultAsync(t => t.Json.Id == id, cancellationToken);

            if (item is null)
            {
                return false;
            }

            onSuccess?.Invoke(item.Json);
            cache?.TryAdd(id, item.Json);
            return true;
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<TesTask>> GetItemsAsync(Expression<Func<TesTask, bool>> predicate, CancellationToken cancellationToken)
        {
            using var dbContext = createDbContext();

            // Search for items in the JSON
            var query = dbContext.TesTasks.Select(t => t.Json).Where(predicate);

            //var sqlQuery = query.ToQueryString();
            //Debugger.Break();
            return await query.ToListAsync(cancellationToken);
        }

        /// <inheritdoc/>
        public async Task<TesTask> CreateItemAsync(TesTask item, CancellationToken cancellationToken)
        {
            using var dbContext = createDbContext();
            var dbItem = new TesTaskDatabaseItem { Json = item };
            dbContext.TesTasks.Add(dbItem);
            await dbContext.SaveChangesAsync(cancellationToken);
            cache?.TryAdd(item.Id, item);
            return item;
        }

        /// <summary>
        /// Encapsulates TesTasks as JSON
        /// </summary>
        /// <param name="item">TesTask to store as JSON in the database</param>
        /// <param name="cancellationToken">A<see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public async Task<List<TesTask>> CreateItemsAsync(List<TesTask> items, CancellationToken cancellationToken)
        {
            using var dbContext = createDbContext();

            foreach (var item in items)
            {
                var dbItem = new TesTaskDatabaseItem { Json = item };
                dbContext.TesTasks.Add(dbItem);
            }

            await dbContext.SaveChangesAsync(cancellationToken);
            return items;
        }

        /// <inheritdoc/>
        /// <remarks>Base class searches within model, this method searches within the JSON</remarks>
        public async Task<TesTask> UpdateItemAsync(TesTask tesTask, CancellationToken cancellationToken)
        {
            var result = await UpdateTesTaskAsync(tesTask);
            cache?.TryUpdate(tesTask.Id, tesTask);
            return result;
        }

        private async Task UpdateTesTasksAsync(IEnumerable<(TesTask TesTask, TaskCompletionSource<TesTask> TaskSource)> updateTesTasks)
        {
            List<TesTaskDatabaseItem> items;
            List<(TesTaskDatabaseItem TesTaskDbItem, TaskCompletionSource<TesTask> TaskSource)> updates = new();

            var list = updateTesTasks.ToList();
            using var dbContext = createDbContext();

            // Manually set entity state to avoid potential NPG PostgreSql bug
            dbContext.ChangeTracker.AutoDetectChangesEnabled = false;

            try
            {
                var ids = list.Select(e => e.TesTask.Id).ToList();
                items = await dbContext.TesTasks.Where(t => ids.Contains(t.Json.Id)).ToListAsync();
            }
            catch (Exception ex)
            {
                FailAll(ex, list.Select(e => e.TaskSource));
                return;
            }

            foreach (var (tesTask, taskSource) in list)
            {
                try
                {
                    var item = items.FirstOrDefault(e => e.Json.Id == tesTask.Id) ?? throw new Exception($"No TesTask with ID {tesTask.Id} found in the database.");
                    item.Json = tesTask;

                    // Manually set entity state to avoid potential NPG PostgreSql bug
                    dbContext.Entry(item).State = EntityState.Modified;
                    updates.Add((item, taskSource));
                }
                catch (Exception ex)
                {
                    // It's expected that if we are here, item is not in updates.
                    taskSource.SetException(ex);
                }
            }

            try
            {
                await dbContext.SaveChangesAsync();
            }
            catch (Exception ex)
            {
                FailAll(ex, updates.Select(e => e.TaskSource));
            }

            foreach (var (item, source) in updates)
            {
                source.SetResult(item.Json);
            }

            static void FailAll(Exception ex, IEnumerable<TaskCompletionSource<TesTask>> sources)
            {
                foreach (var source in sources)
                {
                    source.SetException(ex);
                }
            }
        }

        /// <inheritdoc/>
        /// <remarks>Base class deletes by Item.Id, this method deletes by Item.Json.Id</remarks>
        public async Task DeleteItemAsync(string id, CancellationToken cancellationToken)
        {
            using var dbContext = createDbContext();
            var item = await dbContext.TesTasks.FirstOrDefaultAsync(t => t.Json.Id == id, cancellationToken);

            if (item is null)
            {
                throw new Exception($"No TesTask with ID {item.Id} found in the database.");
            }

            dbContext.TesTasks.Remove(item);
            await dbContext.SaveChangesAsync(cancellationToken);
            cache?.TryRemove(id);
        }

        /// <inheritdoc/>
        /// <remarks>Identical to <see cref="GetItemsAsync(Expression{Func{TesTask, bool}})"/>, paging is not supported. All items are returned, filtered by <paramref name="predicate"/></remarks>
        public async Task<(string, IEnumerable<TesTask>)> GetItemsAsync(Expression<Func<TesTask, bool>> predicate, int pageSize, string continuationToken, CancellationToken cancellationToken)
        {
            // TODO paging support
            var results = await GetItemsAsync(predicate, cancellationToken);
            return (null, results);
        }

        private Task<TesTask> UpdateTesTaskAsync(TesTask tesTask)
        {
            var source = new TaskCompletionSource<TesTask>();
            tasksToUpdate.Enqueue((tesTask, source));
            return source.Task;
        }

        private void UpdaterWorkerProc(object sender, DoWorkEventArgs e)
        {
            while (!updaterWorker.CancellationPending)
            {
                var list = new List<(TesTask TesTask, TaskCompletionSource<TesTask> TaskSource)>();
                while (tasksToUpdate.TryDequeue(out var updateTesTask))
                {
                    list.Add(updateTesTask);
                }

                if (list.Count != 0)
                {
                    UpdateTesTasksAsync(list).Wait();
                }

                Thread.Sleep(1000);
            }
        }

        public void Dispose()
        {
            updaterWorker.CancelAsync();
            while (updaterWorker.IsBusy) { }
            updaterWorker.Dispose();
        }
    }
}
