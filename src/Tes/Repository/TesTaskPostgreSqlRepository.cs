// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Repository
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text.Json;
    using System.Text.Json.Serialization.Metadata;
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
        // JsonSerializerOptions singleton factory
        private static readonly Lazy<JsonSerializerOptions> GetSerializerOptions = new(() =>
        {
            // Create, configure and return JsonSerializerOptions.
            JsonSerializerOptions options = new(JsonSerializerOptions.Default)
            {
                // Be somewhat minimilistic when serializing. Non-null default property values (for any given type) are still written.
                DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,

                // Required since adding modifiers to the default TypeInfoResolver appears to not be possible.
                TypeInfoResolver = GetAndConfigureTypeInfoResolver()
            };

            options.MakeReadOnly(populateMissingResolver: true);
            return options;

            static IJsonTypeInfoResolver GetAndConfigureTypeInfoResolver()
            {
                DefaultJsonTypeInfoResolver typeInfoResolver = new();
                typeInfoResolver.Modifiers.Add(JsonTypeInfoResolverModifier);
                return typeInfoResolver;
            }
        }, LazyThreadSafetyMode.PublicationOnly);

        // DefaultJsonTypeInfoResolver contract modifier to deal with changes to the task JSON.
        // Actions taken:
        //     1) Replace the previous WorkflowId property on TesTask (with the TaskSubmitter implementation).
        //     2) Remove the RegionsAvailable array from VirtualMachineInformation.
        private static void JsonTypeInfoResolverModifier(JsonTypeInfo typeInfo)
        {
            switch (typeInfo.Type)
            {
                case Type type when typeof(TesTask).Equals(type):
                    // Configure tasks created with previous versions of TES when tasks are retrieved.
                    typeInfo.UnmappedMemberHandling ??= System.Text.Json.Serialization.JsonUnmappedMemberHandling.Skip;
                    typeInfo.OnDeserialized ??= obj => ((TesTask)obj).TaskSubmitter ??= TaskSubmitters.TaskSubmitter.Parse((TesTask)obj);
                    break;

                case Type type when typeof(VirtualMachineInformation).Equals(type):
                    // Configure tasks created with previous versions of TES when tasks are retrieved.
                    typeInfo.OnDeserialized ??= obj => ((VirtualMachineInformation)obj).RegionsAvailable = null;
                    break;
            }
        }

        /// <summary>
        /// NpgsqlDataSource factory
        /// </summary>
        public static Func<string, Npgsql.NpgsqlDataSource> NpgsqlDataSourceFunc => connectionString =>
            new Npgsql.NpgsqlDataSourceBuilder(connectionString)
                .ConfigureJsonOptions(serializerOptions: GetSerializerOptions.Value)
                .EnableDynamicJson(jsonbClrTypes: [typeof(TesTask)])
                .Build();

        /// <summary>
        /// Configure options specific to PostgreSQL for a <see cref="DbContext" />
        /// </summary>
        public static void NpgsqlDbContextOptionsBuilder(Npgsql.EntityFrameworkCore.PostgreSQL.Infrastructure.NpgsqlDbContextOptionsBuilder options)
        {
            options.MaxBatchSize(1000);
        }

        /// <summary>
        /// Default constructor that also will create the schema if it does not exist
        /// </summary>
        /// <param name="options"></param>
        /// <param name="hostApplicationLifetime">Used for requesting termination of the current application if the writer task unexpectedly exits.</param>
        /// <param name="logger">Logging interface.</param>
        /// <param name="cache">Memory cache for fast access to active items.</param>
        public TesTaskPostgreSqlRepository(IOptions<PostgreSqlOptions> options, Microsoft.Extensions.Hosting.IHostApplicationLifetime hostApplicationLifetime, ILogger<TesTaskPostgreSqlRepository> logger, ICache<TesTaskDatabaseItem> cache = null)
            : base(hostApplicationLifetime, logger, cache)
        {
            var dataSource = NpgsqlDataSourceFunc(ConnectionStringUtility.GetPostgresConnectionString(options)); // The datasource itself must be essentially a singleton.
            CreateDbContext = Initialize(() => new TesDbContext(dataSource, NpgsqlDbContextOptionsBuilder));
            WarmCacheAsync(CancellationToken.None).GetAwaiter().GetResult();
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
            dbContext.Database.MigrateAsync(CancellationToken.None).GetAwaiter().GetResult();
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
                    var activeTasksCount = (await InternalGetItemsAsync(ct, orderBy: q => q.OrderBy(t => t.Json.CreationTime), efPredicate: task => TesTask.ActiveStates.Contains(task.State))).Count();
                    Logger?.LogInformation("Cache warmed successfully in {TotalSeconds:n3} seconds. Added {TasksAddedCount:n0} items to the cache.", sw.Elapsed.TotalSeconds, activeTasksCount);
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

            onSuccess?.Invoke(CopyTesTask(item).TesTask);
            _ = EnsureActiveItemInCache<TesTask>(item, t => t.Json.Id, t => t.Json.IsActiveState());
            return true;
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<TesTask>> GetItemsAsync(Expression<Func<TesTask, bool>> predicate, CancellationToken cancellationToken)
        {
            return (await InternalGetItemsAsync(cancellationToken, efPredicate: predicate)).Select(t => t.TesTask);
        }

        /// <inheritdoc/>
        public async Task<TesTask> CreateItemAsync(TesTask task, CancellationToken cancellationToken)
        {
            var item = new TesTaskDatabaseItem { Json = task };
            item = await AddUpdateOrRemoveItemInDbAsync(item, WriteAction.Add, cancellationToken);
            return EnsureActiveItemInCache(item, t => t.Json.Id, t => t.Json.IsActiveState(), CopyTesTask).TesTask;
        }

        /// <summary>
        /// Encapsulates TesTasks as JSON
        /// </summary>
        /// <param name="item">TesTask to store as JSON in the database</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        public async Task<List<TesTask>> CreateItemsAsync(List<TesTask> items, CancellationToken cancellationToken)
             => [.. (await Task.WhenAll(items.Select(task => CreateItemAsync(task, cancellationToken))))];

        /// <inheritdoc/>
        public async Task<TesTask> UpdateItemAsync(TesTask tesTask, CancellationToken cancellationToken)
        {
            var item = await GetItemFromCacheOrDatabase(tesTask.Id, true, cancellationToken);
            item.Json = tesTask;
            item = await AddUpdateOrRemoveItemInDbAsync(item, WriteAction.Update, cancellationToken);
            return EnsureActiveItemInCache(item, t => t.Json.Id, t => t.Json.IsActiveState(), CopyTesTask).TesTask;
        }

        /// <inheritdoc/>
        public async Task DeleteItemAsync(string id, CancellationToken cancellationToken)
        {
            _ = await AddUpdateOrRemoveItemInDbAsync(await GetItemFromCacheOrDatabase(id, true, cancellationToken), WriteAction.Delete, cancellationToken);
            _ = Cache?.TryRemove(id);
        }

        /// <inheritdoc/>
        public async Task<IRepository<TesTask>.GetItemsResult> GetItemsAsync(string continuationToken, int pageSize, CancellationToken cancellationToken, FormattableString rawPredicate, Expression<Func<TesTask, bool>> efPredicate)
        {
            var last = long.MinValue;

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

            var results = (await InternalGetItemsAsync(
                    cancellationToken,
                    pagination: q => q.Where(t => t.Id > last).Take(pageSize),
                    orderBy: q => q.OrderBy(t => t.Id),
                    efPredicate: efPredicate,
                    rawPredicate: rawPredicate))
                .ToList();

            return new(GetContinuation(results.Count == pageSize ? results.LastOrDefault().DbId : null), results.Select(t => t.TesTask));

            static string GetContinuation(long? dbId)
                => dbId is null ? null : Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(Newtonsoft.Json.JsonConvert.SerializeObject(dbId)));
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
                    throw new KeyNotFoundException($"No TesTask with ID {id} found in the database.");
                }
            }

            return item;
        }

        /// <summary>
        /// Stands up TesTask query, ensures that active tasks queried are maintained in the cache. Entry point for all non-single task SELECT queries in the repository.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="orderBy"><see cref="IQueryable{TesTaskDatabaseItem}"/> order-by function.</param>
        /// <param name="pagination"><see cref="IQueryable{TesTaskDatabaseItem}"/> pagination selection (within the order-by).</param>
        /// <param name="efPredicate">The WHERE clause <see cref="Expression"/> for <see cref="TesTask"/> selection in the query.</param>
        /// <param name="rawPredicate">The WHERE clause for raw SQL for <see cref="TesTaskDatabaseItem"/> selection in the query.</param>
        /// <returns></returns>
        private async Task<IEnumerable<GetItemsResult>> InternalGetItemsAsync(CancellationToken cancellationToken, Func<IQueryable<TesTaskDatabaseItem>, IQueryable<TesTaskDatabaseItem>> orderBy = default, Func<IQueryable<TesTaskDatabaseItem>, IQueryable<TesTaskDatabaseItem>> pagination = default, Expression<Func<TesTask, bool>> efPredicate = default, FormattableString rawPredicate = default)
        {
            using var dbContext = CreateDbContext();
            return (await GetItemsAsync(dbContext.TesTasks, /*readerFunc, */cancellationToken, orderBy, pagination, WhereTesTask(efPredicate), rawPredicate))
                .Select(item => EnsureActiveItemInCache(item, t => t.Json.Id, t => t.Json.IsActiveState(), CopyTesTask));
        }

        /// <summary>
        /// Transforms <paramref name="predicate"/> into <see cref="Expression{Func{TesTaskDatabaseItem, bool}}"/>.
        /// </summary>
        /// <param name="predicate">A <see cref="Expression{Func{TesTask, bool}}"/> to be transformed.</param>
        /// <returns>A <see cref="Expression{Func{TesTaskDatabaseItem, bool}}"/></returns>
        private static Expression<Func<TesTaskDatabaseItem, bool>> WhereTesTask(Expression<Func<TesTask, bool>> predicate)
        {
            return predicate is null
                ? null
                : (Expression<Func<TesTaskDatabaseItem, bool>>)new ExpressionParameterSubstitute(predicate.Parameters[0], GetTask()).Visit(predicate);

            static Expression<Func<TesTaskDatabaseItem, TesTask>> GetTask() => item => item.Json;
        }

        private record class GetItemsResult(long DbId, TesTask TesTask);

        private GetItemsResult CopyTesTask(TesTaskDatabaseItem item)
        {
            return new(item.Id, Newtonsoft.Json.JsonConvert.DeserializeObject<TesTask>(item.Json.ToJson()));
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
