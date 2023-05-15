// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Repository
{
    using System;
    using System.Collections.Generic;
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
    public sealed class TesTaskPostgreSqlRepository : PostgreSqlCachingRepository<TesTaskDatabaseItem>, IRepository<TesTask>
    {
        /// <summary>
        /// Default constructor that also will create the schema if it does not exist
        /// </summary>
        /// <param name="options"></param>
        /// <param name="logger"></param>
        /// <param name="cache"></param>
        public TesTaskPostgreSqlRepository(IOptions<PostgreSqlOptions> options, ILogger<TesTaskPostgreSqlRepository> logger, ICache<TesTaskDatabaseItem> cache = null)
            : base(logger, cache)
        {
            var connectionString = new ConnectionStringUtility().GetPostgresConnectionString(options);
            CreateDbContext = () => { return new TesDbContext(connectionString); };
            using var dbContext = CreateDbContext();
            dbContext.Database.MigrateAsync().Wait();
            _ = dbContext.Database.ExecuteSqlRawAsync("delete from testasks").Result;
            WarmCacheAsync().Wait();
        }

        /// <summary>
        /// Constructor for testing to enable mocking DbContext 
        /// </summary>
        /// <param name="createDbContext">A delegate that creates a TesTaskPostgreSqlRepository context</param>
        public TesTaskPostgreSqlRepository(Func<TesDbContext> createDbContext)
            : base()
        {
            CreateDbContext = createDbContext;
            using var dbContext = createDbContext();
            dbContext.Database.MigrateAsync().Wait();
        }

        private async Task WarmCacheAsync()
        {
            if (_cache is null)
            {
                _logger?.LogWarning("Cache is null for TesTaskPostgreSqlRepository; no caching will be used.");
                return;
            }

            var sw = Stopwatch.StartNew();
            _logger?.LogInformation("Warming cache...");

            // Don't allow the state of the system to change until the cache and system are consistent;
            // this is a fast PostgreSQL query even for 1 million items
            await Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(3,
                    retryAttempt =>
                    {
                        _logger?.LogWarning("Warming cache retry attempt #{RetryAttempt}", retryAttempt);
                        return TimeSpan.FromSeconds(10);
                    },
                    (ex, ts) =>
                    {
                        _logger?.LogCritical(ex, "Couldn't warm cache, is the database online?");
                    })
                .ExecuteAsync(async () =>
                {
                    var activeTasksCount = (await InternalGetItemsAsync(task => TesTask.ActiveStates.Contains(task.State), CancellationToken.None, q => q.OrderBy(t => t.Json.CreationTime))).Count();
                    _logger?.LogInformation("Cache warmed successfully in {TotalSeconds} seconds. Added {TasksAddedCount} items to the cache.", $"{sw.Elapsed.TotalSeconds:n3}", $"{activeTasksCount:n0}");
                });
        }


        /// <summary>
        /// Get a TesTask by the TesTask.ID
        /// </summary>
        /// <param name="id">The TesTask's ID</param>
        /// <param name="onSuccess">Delegate to be invoked on success</param>
        /// <returns></returns>
        public async Task<bool> TryGetItemAsync(string id, Action<TesTask> onSuccess = null)
        {
            var item = await GetItemFromCacheOrDatabase(id, false, CancellationToken.None);

            if (item is null)
            {
                return false;
            }

            onSuccess?.Invoke(item.Json);
            _ = EnsureActiveItemInCache(item, t => t.Json.Id, t => t.Json.IsActiveState());
            return true;
        }

        /// <summary>
        /// Get TesTask items
        /// </summary>
        /// <param name="predicate">Predicate to query the TesTasks</param>
        /// <returns></returns>
        public async Task<IEnumerable<TesTask>> GetItemsAsync(Expression<Func<TesTask, bool>> predicate)
            => (await InternalGetItemsAsync(predicate, CancellationToken.None));

        /// <summary>
        /// Encapsulates a TesTask as JSON
        /// </summary>
        /// <param name="item">TesTask to store as JSON in the database</param>
        /// <returns></returns>
        public async Task<TesTask> CreateItemAsync(TesTask task)
        {
            var item = new TesTaskDatabaseItem { Json = task };
            item = await AddUpdateOrRemoveItemInDbAsync(item, WriteAction.Add);
            return EnsureActiveItemInCache(item, t => t.Json.Id, t => t.Json.IsActiveState()).Json;
        }

        /// <summary>
        /// Encapsulates TesTasks as JSON
        /// </summary>
        /// <param name="item">TesTask to store as JSON in the database</param>
        /// <returns></returns>
        public async Task<List<TesTask>> CreateItemsAsync(List<TesTask> items)
             => (await Task.WhenAll(items.Select(CreateItemAsync))).ToList();

        /// <summary>
        /// Base class searches within model, this method searches within the JSON
        /// </summary>
        /// <param name="tesTask"></param>
        /// <returns></returns>
        public async Task<TesTask> UpdateItemAsync(TesTask tesTask)
        {
            var item = await GetItemFromCacheOrDatabase(tesTask.Id, true, CancellationToken.None);
            item.Json = tesTask;
            item = await AddUpdateOrRemoveItemInDbAsync(item, WriteAction.Update);
            return EnsureActiveItemInCache(item, t => t.Json.Id, t => t.Json.IsActiveState()).Json;
        }

        /// <summary>
        /// Base class deletes by Item.Id, this method deletes by Item.Json.Id
        /// </summary>
        /// <param name="id">TesTask Id</param>
        /// <returns></returns>
        public async Task DeleteItemAsync(string id)
        {
            _ = await AddUpdateOrRemoveItemInDbAsync(await GetItemFromCacheOrDatabase(id, true, CancellationToken.None), WriteAction.Delete);
            _ = _cache?.TryRemove(id);
        }

        /// <summary>
        /// Identical to GetItemsAsync, paging is not supported. All items are returned
        /// </summary>
        /// <param name="predicate">Predicate to query the TesTasks</param>
        /// <param name="pageSize">Ignored and has no effect</param>
        /// <param name="continuationToken">Ignored and has no effect</param>
        /// <returns></returns>
        public async Task<(string, IEnumerable<TesTask>)> GetItemsAsync(Expression<Func<TesTask, bool>> predicate, int pageSize, string continuationToken)
        {
            var last = (CreationTime: DateTimeOffset.MinValue, Id: string.Empty);

            if (continuationToken is not null)
            {
                try
                {
                    var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(256);
                    if (Convert.TryFromBase64String(continuationToken, buffer, out var bytesWritten))
                    {
                        last = Newtonsoft.Json.JsonConvert.DeserializeAnonymousType(System.Text.Encoding.UTF8.GetString(buffer, 0, bytesWritten), last);
                    }

                    if (last == default)
                    {
                        throw new ArgumentException("pageToken is corrupt or invalid.", nameof(continuationToken));
                    }
                }
                catch (System.Text.DecoderFallbackException ex)
                {
                    throw new ArgumentException("pageToken is corrupt or invalid.", nameof(continuationToken), ex);
                }
                catch (Newtonsoft.Json.JsonException ex)
                {
                    throw new ArgumentException("pageToken is corrupt or invalid.", nameof(continuationToken), ex);
                }
            }

            // This "uglyness" should (hopefully) be fixed in EF8: https://github.com/dotnet/roslyn/issues/12897 reference https://github.com/dotnet/efcore/issues/26822 when we can compare last directly with a created per-item tuple
            //var results = (await InternalGetItemsAsync(predicate, cancellationToken, q => q.Where(t => t.Json.CreationTime > last.CreationTime || (t.Json.CreationTime == last.CreationTime && t.Json.Id.CompareTo(last.Id) > 0)).Take(pageSize))).ToList();
            var results = (await InternalGetItemsAsync(predicate, CancellationToken.None, pagination: q => q.Where(t => t.Json.Id.CompareTo(last.Id) > 0).Take(pageSize))).ToList();

            return (GetContinuation(results.Count == pageSize ? results.LastOrDefault() : null), results);

            static string GetContinuation(TesTask item)
                => item is null ? null : Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(Newtonsoft.Json.JsonConvert.SerializeObject((item.CreationTime, item.Id))));
        }

        /// <summary>
        /// Retrieves an item from the cache if found, otherwise from the database.
        /// </summary>
        /// <param name="id">TesTask Id</param>
        /// <param name="throwIfNotFound">Throw (instead of return null) if item is not found.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>The found item, or null if not found.</returns>
        private async Task<TesTaskDatabaseItem> GetItemFromCacheOrDatabase(string id, bool throwIfNotFound, CancellationToken cancellationToken)
        {
            TesTaskDatabaseItem item = default;

            if (!_cache?.TryGetValue(id, out item) ?? true)
            {
                using var dbContext = CreateDbContext();

                // Search for Id within the JSON
                item = await _asyncPolicy.ExecuteAsync(ct => dbContext.TesTasks.FirstOrDefaultAsync(t => t.Json.Id == id, ct), cancellationToken);

                if (throwIfNotFound && item is null)
                {
                    throw new KeyNotFoundException($"No TesTask with ID {item.Id} found in the database.");
                }
            }

            return item;
        }

        /// <summary>
        /// Stands up TesTask query, ensures that active tasks queried are maintained in the cache. Entry point for all non-single task SELECT queries in the repository.
        /// </summary>
        /// <param name="predicate"></param>
        /// <param name="cancellationToken"></param>
        /// <param name="orderBy"></param>
        /// <param name="pagination"></param>
        /// <returns></returns>
        private async Task<IEnumerable<TesTask>> InternalGetItemsAsync(Expression<Func<TesTask, bool>> predicate, CancellationToken cancellationToken, Func<IQueryable<TesTaskDatabaseItem>, IQueryable<TesTaskDatabaseItem>> orderBy = default, Func<IQueryable<TesTaskDatabaseItem>, IQueryable<TesTaskDatabaseItem>> pagination = default)
        {
            // It turns out, PostgreSQL doesn't handle EF's interpretation of ORDER BY more then one "column" in any resonable way, so we have to order by the only thing we have that is expected to be unique.
            //orderBy = pagination is null ? orderBy : q => q.OrderBy(t => t.Json.CreationTime).ThenBy(t => t.Json.Id);
            orderBy = pagination is null ? orderBy : q => q.OrderBy(t => t.Json.Id);

            using var dbContext = CreateDbContext();
            return (await GetItemsAsync(dbContext.TesTasks, WhereTesTask(predicate), cancellationToken, orderBy, pagination)).Select(item => EnsureActiveItemInCache(item, t => t.Json.Id, t => t.Json.IsActiveState()).Json);
        }

        /// <summary>
        /// Transforms <paramref name="predicate"/> into <see cref="Expression{Func{TesTaskDatabaseItem, bool}}"/>.
        /// </summary>
        /// <param name="predicate">A <see cref="Expression{Func{TesTask, bool}}"/> to be transformed.</param>
        /// <returns>A <see cref="Expression{Func{TesTaskDatabaseItem, bool}}"/></returns>
        private static Expression<Func<TesTaskDatabaseItem, bool>> WhereTesTask(Expression<Func<TesTask, bool>> predicate)
        {
            return (Expression<Func<TesTaskDatabaseItem, bool>>)new ExpressionParameterSubstitute(predicate.Parameters[0], GetTask()).Visit(predicate);

            static Expression<Func<TesTaskDatabaseItem, TesTask>> GetTask() => item => item.Json;
        }

        public ValueTask<bool> TryRemoveItemFromCacheAsync(TesTask item)
        {
            return ValueTask.FromResult(_cache?.TryRemove(item.Id) ?? false);
        }
    }
}
