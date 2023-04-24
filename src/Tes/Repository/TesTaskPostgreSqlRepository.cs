// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Repository
{
    using System;
    using System.Buffers;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Text;
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
    public sealed class TesTaskPostgreSqlRepository : PostgreSqlCachingRepository<TesTaskDatabaseItem>, IRepository<TesTask>
    {
        /// <summary>
        /// Default constructor that also will create the schema if it does not exist
        /// </summary>
        /// <param name="connectionString">The PostgreSql connection string</param>
        public TesTaskPostgreSqlRepository(IOptions<PostgreSqlOptions> options, ILogger<TesTaskPostgreSqlRepository> logger, ICache<TesTaskDatabaseItem> cache)
            : base(cache, logger)
        {
            var connectionString = new ConnectionStringUtility().GetPostgresConnectionString(options);
            createDbContext = () => { return new TesDbContext(connectionString); };
            using var dbContext = createDbContext();
            dbContext.Database.MigrateAsync().Wait();
            WarmCacheAsync().Wait();
        }

        /// <summary>
        /// Constructor for testing to enable mocking DbContext 
        /// </summary>
        /// <param name="createDbContext">A delegate that creates a TesTaskPostgreSqlRepository context</param>
        public TesTaskPostgreSqlRepository(Func<TesDbContext> createDbContext, ICache<TesTaskDatabaseItem> cache)
            : base(cache, default)
        {
            this.createDbContext = createDbContext;
            using var dbContext = createDbContext();
            dbContext.Database.MigrateAsync().Wait();
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
                        logger.LogWarning("Warming cache retry attempt #{RetryAttempt}", retryAttempt);
                        return TimeSpan.FromSeconds(10);
                    },
                    (ex, ts) =>
                    {
                        logger.LogCritical(ex, "Couldn't warm cache, is the database online?");
                    })
                .ExecuteAsync(async () =>
                {
                    var activeTasks = await InternalGetItemsAsync(task => TesTask.ActiveStates.Contains(task.State), q => q, CancellationToken.None);
                    var tasksAddedCount = 0;

                    foreach (var task in activeTasks.OrderBy(t => t.Json.CreationTime))
                    {
                        cache.TryAdd(task.Json.Id, task);
                        tasksAddedCount++;
                    }

                    logger.LogInformation("Cache warmed successfully in {TotalSeconds} seconds. Added {TasksAddedCount} items to the cache.", $"{sw.Elapsed.TotalSeconds:n3}", $"{tasksAddedCount:n0}");
                });
        }

        /// <inheritdoc/>
        public async Task<bool> TryGetItemAsync(string id, Action<TesTask> onSuccess, CancellationToken cancellationToken)
        {
            var item = await GetItemFromCacheOrDatabase(id, cancellationToken);

            if (item is null)
            {
                return false;
            }

            onSuccess?.Invoke(item.Json);
            _ = EnsureActiveTaskInCache(item, t => t.Json.Id, t => t.Json.IsActiveState());
            return true;
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<TesTask>> GetItemsAsync(Expression<Func<TesTask, bool>> predicate, CancellationToken cancellationToken)
            => (await InternalGetItemsAsync(predicate, q => q, cancellationToken)).Select(t => t.Json);

        /// <inheritdoc/>
        public async Task<TesTask> CreateItemAsync(TesTask task, CancellationToken cancellationToken)
        {
            TesTaskDatabaseItem item = new() { Json = task };
            item = await AddUpdateOrRemoveTaskInDbAsync(item, WriteAction.Add);
            return EnsureActiveTaskInCache(item, t => t.Json.Id, t => t.Json.IsActiveState()).Json;
        }

        /// <summary>
        /// Encapsulates TesTasks as JSON
        /// </summary>
        /// <param name="item">TesTask to store as JSON in the database</param>
        /// <param name="cancellationToken">A<see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public async Task<List<TesTask>> CreateItemsAsync(List<TesTask> items, CancellationToken cancellationToken)
            => (await Task.WhenAll(items.Select(item => CreateItemAsync(item, cancellationToken)))).ToList();

        private async Task<TesTaskDatabaseItem> GetItemFromCacheOrDatabase(string id, CancellationToken cancellationToken)
        {
            if (!cache.TryGetValue(id, out var item))
            {
                using var dbContext = createDbContext();

                // Search for Id within the JSON
                item = await dbContext.TesTasks.FirstOrDefaultAsync(t => t.Json.Id == id, cancellationToken);

                if (item is null)
                {
                    throw new KeyNotFoundException($"No TesTask with ID {id} found in the database.");
                }
            }

            return item;
        }

        /// <inheritdoc/>
        /// <remarks>Base class searches within model, this method searches within the JSON</remarks>
        public async Task<TesTask> UpdateItemAsync(TesTask tesTask, CancellationToken cancellationToken)
        {
            var item = await GetItemFromCacheOrDatabase(tesTask.Id, cancellationToken);
            item.Json = tesTask;
            item = await AddUpdateOrRemoveTaskInDbAsync(item, WriteAction.Update);
            return EnsureActiveTaskInCache(item, t => t.Json.Id, t => t.Json.IsActiveState()).Json;
        }

        /// <inheritdoc/>
        /// <remarks>Base class deletes by Item.Id, this method deletes by Item.Json.Id</remarks>
        public async Task DeleteItemAsync(string id, CancellationToken cancellationToken)
        {
            _ = await AddUpdateOrRemoveTaskInDbAsync(await GetItemFromCacheOrDatabase(id, cancellationToken), WriteAction.Delete);
            cache.TryRemove(id);
        }

        /// <inheritdoc/>
        public async Task<(string, IEnumerable<TesTask>)> GetItemsAsync(Expression<Func<TesTask, bool>> predicate, string continuationToken, int pageSize, CancellationToken cancellationToken)
        {
            var last = 0; // (DateTimeOffset.MinValue, string.Empty);
            if (continuationToken is not null)
            {
                try
                {
                    var buffer = ArrayPool<byte>.Shared.Rent(256);
                    if (Convert.TryFromBase64String(continuationToken, buffer, out var bytesWritten))
                    {
                        last = Newtonsoft.Json.JsonConvert.DeserializeAnonymousType(Encoding.UTF8.GetString(buffer, 0, bytesWritten), last);
                    }

                    if (last == default)
                    {
                        throw new ArgumentException("pageToken is corrupt or invalid", nameof(continuationToken));
                    }
                }
                catch (DecoderFallbackException ex)
                {
                    throw new ArgumentException("pageToken is corrupt or invalid", nameof(continuationToken), ex);
                }
                catch (Newtonsoft.Json.JsonException ex)
                {
                    throw new ArgumentException("pageToken is corrupt or invalid", nameof(continuationToken), ex);
                }
            }

            // This "uglyness" of wrapping/unwrapping Select() calls and calling EF functions directly should (hopefully) be fixed in EF8: https://github.com/dotnet/roslyn/issues/12897 reference https://github.com/dotnet/efcore/issues/26822
            var results = (await InternalGetItemsAsync(predicate, q => q.Where(t => t.Id > last).Take(pageSize), cancellationToken)).Select(t => t.Json).ToList();
            var lastItem = results.Count == pageSize ? results.LastOrDefault() : null;
            return (lastItem is null ? null : Convert.ToBase64String(Encoding.UTF8.GetBytes(Newtonsoft.Json.JsonConvert.SerializeObject((await GetItemFromCacheOrDatabase(lastItem.Id, cancellationToken)).Id/*(lastItem.CreationTime, lastItem.Id)*/))), results);
        }

        private async Task<IEnumerable<TesTaskDatabaseItem>> InternalGetItemsAsync(Expression<Func<TesTask, bool>> predicate, Func<IQueryable<TesTaskDatabaseItem>, IQueryable<TesTaskDatabaseItem>> pagination, CancellationToken cancellationToken)
        {
            using var dbContext = createDbContext();
            return (await GetItemsAsync(dbContext.TesTasks, WhereTesTask(predicate), q => q.OrderBy(t => t.Id)/*q.OrderBy(t => t.Json.CreationTime).ThenBy(t => t.Json.Id)*/, pagination, cancellationToken)).Select(item => EnsureActiveTaskInCache(item, t => t.Json.Id, t => t.Json.IsActiveState()));
        }

        private static Expression<Func<TesTaskDatabaseItem, bool>> WhereTesTask(Expression<Func<TesTask, bool>> predicate)
        {
            return (Expression<Func<TesTaskDatabaseItem, bool>>)new ExpressionParameterSubstitute(predicate.Parameters[0], GetTask()).Visit(predicate);

            static Expression<Func<TesTaskDatabaseItem, TesTask>> GetTask()
                => item => item.Json;
        }
    }
}
