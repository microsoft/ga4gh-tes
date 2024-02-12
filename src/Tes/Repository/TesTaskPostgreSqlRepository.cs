// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Repository
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Diagnostics;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Runtime.Serialization;
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
        // Creator of NpgsqlDataSource
        public static Func<string, Npgsql.NpgsqlDataSource> NpgsqlDataSourceBuilder
            => connectionString => new Npgsql.NpgsqlDataSourceBuilder(connectionString)
                            .EnableDynamicJson(jsonbClrTypes: new[] { typeof(TesTask) })
                            .Build();

        // Configuration of NpgsqlDbContext
        public static Action<Npgsql.EntityFrameworkCore.PostgreSQL.Infrastructure.NpgsqlDbContextOptionsBuilder> NpgsqlDbContextOptionsBuilder => options =>
            options.MaxBatchSize(1000);

        /// <summary>
        /// Default constructor that also will create the schema if it does not exist
        /// </summary>
        /// <param name="options"></param>
        /// <param name="hostApplicationLifetime">Used for requesting termination of the current application if the writer task unexpectedly exits.</param>
        /// <param name="logger"></param>
        /// <param name="cache"></param>
        public TesTaskPostgreSqlRepository(IOptions<PostgreSqlOptions> options, Microsoft.Extensions.Hosting.IHostApplicationLifetime hostApplicationLifetime, ILogger<TesTaskPostgreSqlRepository> logger, ICache<TesTaskDatabaseItem> cache = null)
            : base(hostApplicationLifetime, logger, cache)
        {
            var npgsqlDataSource = NpgsqlDataSourceBuilder(ConnectionStringUtility.GetPostgresConnectionString(options)); // This must be run just once, do not move it into the lambda below.
            CreateDbContext = Initialize(() => new TesDbContext(npgsqlDataSource, NpgsqlDbContextOptionsBuilder));
            WarmCacheAsync(CancellationToken.None).Wait();
        }

        /// <summary>
        /// Constructor for testing to enable mocking DbContext 
        /// </summary>
        /// <param name="createDbContext">A delegate that creates a TesTaskPostgreSqlRepository context</param>
        public TesTaskPostgreSqlRepository(Func<TesDbContext> createDbContext)
            : base(default)
        {
            CreateDbContext = Initialize(createDbContext);
        }

        private static Func<TesDbContext> Initialize(Func<TesDbContext> createDbContext)
        {
            using var dbContext = createDbContext();
            dbContext.Database.MigrateAsync(CancellationToken.None).Wait();
            return createDbContext;
        }

        private async Task WarmCacheAsync(CancellationToken cancellationToken)
        {
            if (Cache is null)
            {
                Logger?.LogWarning("Cache is null for TesTaskPostgreSqlRepository; no caching will be used.");
                return;
            }

            var sw = Stopwatch.StartNew();
            Logger?.LogInformation("Warming cache...");

            // Don't allow the state of the system to change until the cache and system are consistent;
            // this is a fast PostgreSQL query even for 1 million items
            await Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(3,
                    retryAttempt =>
                    {
                        Logger?.LogWarning("Warming cache retry attempt #{RetryAttempt}", retryAttempt);
                        return TimeSpan.FromSeconds(10);
                    },
                    (ex, ts) =>
                    {
                        Logger?.LogCritical(ex, "Couldn't warm cache, is the database online?");
                    })
                .ExecuteAsync(async ct =>
                {
                    var activeTasksCount = (await InternalGetItemsAsync(ct, q => q.OrderBy(t => t.Json.CreationTime), efPredicate: task => TesTask.ActiveStates.Contains(task.State))).Count();
                    Logger?.LogInformation("Cache warmed successfully in {TotalSeconds} seconds. Added {TasksAddedCount} items to the cache.", $"{sw.Elapsed.TotalSeconds:n3}", $"{activeTasksCount:n0}");
                }, cancellationToken);
        }


        /// <inheritdoc/>
        public async Task<bool> TryGetItemAsync(string id, CancellationToken cancellationToken, Action<TesTask> onSuccess = null)
        {
            var item = await GetItemFromCacheOrDatabase(id, false, cancellationToken);

            if (item is null)
            {
                return false;
            }

            onSuccess?.Invoke(item.Json);
            _ = EnsureActiveItemInCache<TesTask>(item, t => t.Json.Id, t => t.Json.IsActiveState());
            return true;
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<TesTask>> GetItemsAsync(Expression<Func<TesTask, bool>> predicate, CancellationToken cancellationToken)
        {
            return await InternalGetItemsAsync(cancellationToken, efPredicate: predicate);
        }

        /// <inheritdoc/>
        public async Task<TesTask> CreateItemAsync(TesTask task, CancellationToken cancellationToken)
        {
            var item = new TesTaskDatabaseItem { Json = task };
            item = await AddUpdateOrRemoveItemInDbAsync(item, WriteAction.Add, cancellationToken);
            return EnsureActiveItemInCache(item, t => t.Json.Id, t => t.Json.IsActiveState(), CopyTesTask);
        }

        /// <summary>
        /// Encapsulates TesTasks as JSON
        /// </summary>
        /// <param name="item">TesTask to store as JSON in the database</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        public async Task<List<TesTask>> CreateItemsAsync(List<TesTask> items, CancellationToken cancellationToken)
             => (await Task.WhenAll(items.Select(task => CreateItemAsync(task, cancellationToken)))).ToList();

        /// <inheritdoc/>
        public async Task<TesTask> UpdateItemAsync(TesTask tesTask, CancellationToken cancellationToken)
        {
            var item = await GetItemFromCacheOrDatabase(tesTask.Id, true, cancellationToken);
            item.Json = tesTask;
            item = await AddUpdateOrRemoveItemInDbAsync(item, WriteAction.Update, cancellationToken);
            return EnsureActiveItemInCache(item, t => t.Json.Id, t => t.Json.IsActiveState(), CopyTesTask);
        }

        /// <inheritdoc/>
        public async Task DeleteItemAsync(string id, CancellationToken cancellationToken)
        {
            _ = await AddUpdateOrRemoveItemInDbAsync(await GetItemFromCacheOrDatabase(id, true, cancellationToken), WriteAction.Delete, cancellationToken);
            _ = Cache?.TryRemove(id);
        }

        /// <inheritdoc/>
        public async Task<(string, IEnumerable<TesTask>)> GetItemsAsync(string continuationToken, int pageSize, CancellationToken cancellationToken, FormattableString rawPredicate, Expression<Func<TesTask, bool>> efPredicate)
        {
            var last = (CreationTime: DateTimeOffset.MinValue, Id: long.MinValue); // Temporary, we'd like to use the task Id but for now we're using the database table's Id. Since it's a bigint, this is the default value we'll use.

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

            // As it turns out, PostgreSQL doesn't handle EF's interpretation of ORDER BY more then one "column" in any resonable way, so we will currently have to order by the only thing we have that is expected to be unique (unless we want to code it ourselves or if EF8 also fixes this.
            //orderBy: q => q.OrderBy(t => t.Json.CreationTime).ThenBy(t => t.Json.Id);

            // This "uglyness" should (hopefully) be fixed in EF8: https://github.com/dotnet/roslyn/issues/12897 reference https://github.com/dotnet/efcore/issues/26822 when we can compare last directly with a created per-item tuple
            //var results = (await InternalGetItemsAsync(predicate, cancellationToken, q => q.Where(t => t.Json.CreationTime > last.CreationTime || (t.Json.CreationTime == last.CreationTime && t.Json.Id.CompareTo(last.Id) > 0)).Take(pageSize))).ToList();
            var results = (await InternalGetItemsAsync(cancellationToken, pagination: q => q.Where(t => t.Id.CompareTo(last.Id) > 0).Take(pageSize), orderBy: q => q.OrderBy(t => t.Id), efPredicate: efPredicate, rawPredicate: rawPredicate)).ToList();

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

            if (!Cache?.TryGetValue(id, out item) ?? true)
            {
                using var dbContext = CreateDbContext();

                // Search for Id within the JSON
                item = await asyncPolicy.ExecuteAsync(ct => dbContext.TesTasks.FirstOrDefaultAsync(t => t.Json.Id == id, ct), cancellationToken);

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
        /// <param name="cancellationToken"></param>
        /// <param name="orderBy"></param>
        /// <param name="pagination"></param>
        /// <param name="efPredicate"></param>
        /// <param name="rawPredicate"></param>
        /// <returns></returns>
        private async Task<IEnumerable<TesTask>> InternalGetItemsAsync(CancellationToken cancellationToken, Func<IQueryable<TesTaskDatabaseItem>, IQueryable<TesTaskDatabaseItem>> orderBy = default, Func<IQueryable<TesTaskDatabaseItem>, IQueryable<TesTaskDatabaseItem>> pagination = default, Expression<Func<TesTask, bool>> efPredicate = default, FormattableString rawPredicate = default)
        {
            var readerFunc = new Func<DbDataReader, TesTaskDatabaseItem>(reader => new TesTaskDatabaseItem
            {
                Id = reader.IsDBNull(0) ? default : reader.GetInt64(0),
                Json = System.Text.Json.JsonSerializer.Deserialize<TesTask>(reader.IsDBNull(1) ? default : reader.GetString(1)),
            });

            using var dbContext = CreateDbContext();
            return (await GetItemsAsync(dbContext.TesTasks, readerFunc, cancellationToken, orderBy, pagination, efPredicate is null ? null : WhereTesTask(efPredicate), rawPredicate)).Select(item => EnsureActiveItemInCache(item, t => t.Json.Id, t => t.Json.IsActiveState(), CopyTesTask));
        }

        /// <summary>
        /// Transforms <paramref name="predicate"/> into <see cref="Expression{Func{TesTaskDatabaseItem, bool}}"/>.
        /// </summary>
        /// <param name="predicate">A <see cref="Expression{Func{TesTask, bool}}"/> to be transformed.</param>
        /// <returns>A <see cref="Expression{Func{TesTaskDatabaseItem, bool}}"/></returns>
        private static Expression<Func<TesTaskDatabaseItem, bool>> WhereTesTask(Expression<Func<TesTask, bool>> predicate)
        {
            ArgumentNullException.ThrowIfNull(predicate);

            return (Expression<Func<TesTaskDatabaseItem, bool>>)new ExpressionParameterSubstitute(predicate.Parameters[0], GetTask()).Visit(predicate);

            static Expression<Func<TesTaskDatabaseItem, TesTask>> GetTask() => item => item.Json;
        }

        private TesTask CopyTesTask(TesTaskDatabaseItem item)
        {
            using var stream = new System.IO.MemoryStream();
            var serializer = new DataContractSerializer(typeof(TesTask));
            serializer.WriteObject(stream, item.Json);
            stream.Seek(0, System.IO.SeekOrigin.Begin);
            return (TesTask)serializer.ReadObject(stream);
        }

        /// <inheritdoc/>
        public ValueTask<bool> TryRemoveItemFromCacheAsync(TesTask item, CancellationToken _1)
        {
            return ValueTask.FromResult(Cache?.TryRemove(item.Id) ?? false);
        }

        /// <inheritdoc/>
        public FormattableString JsonFormattableRawString(string property, FormattableString sql)
        {
            var column = typeof(TesTaskDatabaseItem).GetProperty(nameof(TesTaskDatabaseItem.Json))?.GetCustomAttribute<System.ComponentModel.DataAnnotations.Schema.ColumnAttribute>()?.Name ?? "json";
            return new PrependableFormattableString($"\"{column}\"->'{property}'", sql);
        }
    }
}
