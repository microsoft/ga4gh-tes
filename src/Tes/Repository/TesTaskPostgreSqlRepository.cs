// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Repository
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Linq.Expressions;
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
        // Creator of JsonSerializerOptions
        private static readonly Lazy<JsonSerializerOptions> GetSerializerOptions = new(() =>
        {
            // Create JsonSerializerOptions
            JsonSerializerOptions options = new(JsonSerializerOptions.Default)
            {
                // Be somewhat minimilistic when storing data in the repository.
                DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,

                // Create and configure TypeInfoResolver.
                TypeInfoResolver = new InheritedPolymorphismResolver()
            };

            // Resolver contract updates
            ((DefaultJsonTypeInfoResolver)options.TypeInfoResolver).Modifiers.Add(TesTaskJsonTypeInfoResolverModifier);

            return options;
        }, LazyThreadSafetyMode.PublicationOnly);

        private static void TesTaskJsonTypeInfoResolverModifier(JsonTypeInfo typeInfo)
        {
            switch (typeInfo.Type)
            {
                case Type type when typeof(TesTask).Equals(type):
                    // Configure tasks created with previous versions of TES when tasks are retrieved. This does not automatically cause the task to be updated in the repository.
                    typeInfo.OnDeserialized = obj => ((TesTask)obj).TaskSubmitter ??= TaskSubmitter.Parse((TesTask)obj);
                    break;

                case Type type when new[] { typeof(UnknownTaskSubmitter), typeof(CromwellTaskSubmitter) }.Contains(type):
                    typeInfo.CreateObject ??= () => Activator.CreateInstance(type); // Apparent bug. However, it doesn't manifest outside of E/F. TODO: open issue somewhere.
                    break;

                case Type type when typeof(TaskSubmitter).Equals(type):
                    typeInfo.CreateObject ??= () => null!; // Apparent bug. However, it doesn't manifest outside of E/F. TODO: open issue somewhere.
                    break;
            }
        }

        // based on https://github.com/dotnet/runtime/issues/77532#issuecomment-1300541631
        private sealed class InheritedPolymorphismResolver : DefaultJsonTypeInfoResolver
        {
            /// <inheritdoc/>
            public override JsonTypeInfo GetTypeInfo(Type type, JsonSerializerOptions options)
            {
                ArgumentNullException.ThrowIfNull(type);
                ArgumentNullException.ThrowIfNull(options);

                var typeInfo = base.GetTypeInfo(type, options);

                if (typeInfo.PolymorphismOptions is null)
                {
                    // Only handles class hierarchies -- interface hierarchies left out intentionally here
                    for (var baseType = type; !baseType.IsSealed && baseType.BaseType is not null; baseType = baseType.BaseType)
                    {
                        // recursively resolve metadata for the base type and extract any derived type declarations that overlap with the current type
                        if (base.GetTypeInfo(baseType.BaseType, options).PolymorphismOptions is JsonPolymorphismOptions basePolymorphismOptions)
                        {
                            foreach (var derivedType in basePolymorphismOptions.DerivedTypes)
                            {
                                if (type.IsAssignableFrom(derivedType.DerivedType))
                                {
                                    typeInfo.PolymorphismOptions ??= new()
                                    {
                                        IgnoreUnrecognizedTypeDiscriminators = basePolymorphismOptions.IgnoreUnrecognizedTypeDiscriminators,
                                        TypeDiscriminatorPropertyName = basePolymorphismOptions.TypeDiscriminatorPropertyName,
                                        UnknownDerivedTypeHandling = basePolymorphismOptions.UnknownDerivedTypeHandling,
                                    };

                                    typeInfo.PolymorphismOptions.DerivedTypes.Add(derivedType);
                                }
                            }

                            return typeInfo;
                        }
                    }
                }

                return typeInfo;
            }
        }


        // Creator of NpgsqlDataSource
        public static Lazy<Func<string, Npgsql.NpgsqlDataSource>> NpgsqlDataSourceBuilder => new(
            () => connectionString => new Npgsql.NpgsqlDataSourceBuilder(connectionString)
                            .ConfigureJsonOptions(serializerOptions: GetSerializerOptions.Value)
                            .EnableDynamicJson(jsonbClrTypes: [typeof(TesTask)])
                            .Build(),
            LazyThreadSafetyMode.PublicationOnly);

        // Configuration of NpgsqlDbContext
        public static Lazy<Action<Npgsql.EntityFrameworkCore.PostgreSQL.Infrastructure.NpgsqlDbContextOptionsBuilder>> NpgsqlDbContextOptionsBuilder => new(
            () => options => options.MaxBatchSize(1000),
            LazyThreadSafetyMode.PublicationOnly);

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
            CreateDbContext = Initialize(() => new TesDbContext(NpgsqlDataSourceBuilder.Value(ConnectionStringUtility.GetPostgresConnectionString(options)), NpgsqlDbContextOptionsBuilder.Value));
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
                    var activeTasksCount = (await InternalGetItemsAsync(task => TesTask.ActiveStates.Contains(task.State), ct, q => q.OrderBy(t => t.Json.CreationTime))).Count();
                    Logger?.LogInformation("Cache warmed successfully in {TotalSeconds:n3} seconds. Added {TasksAddedCount:n0} items to the cache.", sw.Elapsed.TotalSeconds, activeTasksCount);
                }, cancellationToken);
        }


        /// <summary>
        /// Get a TesTask by the TesTask.ID
        /// </summary>
        /// <param name="id">The TesTask's ID</param>
        /// <param name="cancellationToken"></param>
        /// <param name="onSuccess">Delegate to be invoked on success</param>
        /// <returns></returns>
        public async Task<bool> TryGetItemAsync(string id, CancellationToken cancellationToken, Action<TesTask> onSuccess = null)
        {
            var item = await GetItemFromCacheOrDatabase(id, false, cancellationToken);

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
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<IEnumerable<TesTask>> GetItemsAsync(Expression<Func<TesTask, bool>> predicate, CancellationToken cancellationToken)
        {
            return await InternalGetItemsAsync(predicate, cancellationToken);
        }

        /// <summary>
        /// Encapsulates a TesTask as JSON
        /// </summary>
        /// <param name="item">TesTask to store as JSON in the database</param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<TesTask> CreateItemAsync(TesTask task, CancellationToken cancellationToken)
        {
            var item = new TesTaskDatabaseItem { Json = task };
            item = await AddUpdateOrRemoveItemInDbAsync(item, WriteAction.Add, cancellationToken);
            return EnsureActiveItemInCache(item, t => t.Json.Id, t => t.Json.IsActiveState()).Json;
        }

        /// <summary>
        /// Encapsulates TesTasks as JSON
        /// </summary>
        /// <param name="item">TesTask to store as JSON in the database</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public async Task<List<TesTask>> CreateItemsAsync(List<TesTask> items, CancellationToken cancellationToken)
             => (await Task.WhenAll(items.Select(task => CreateItemAsync(task, cancellationToken)))).ToList();

        /// <summary>
        /// Base class searches within model, this method searches within the JSON
        /// </summary>
        /// <param name="tesTask"></param>
        /// <returns></returns>
        /// <param name="cancellationToken"></param>
        public async Task<TesTask> UpdateItemAsync(TesTask tesTask, CancellationToken cancellationToken)
        {
            var item = await GetItemFromCacheOrDatabase(tesTask.Id, true, cancellationToken);
            item.Json = tesTask;
            item = await AddUpdateOrRemoveItemInDbAsync(item, WriteAction.Update, cancellationToken);
            return EnsureActiveItemInCache(item, t => t.Json.Id, t => t.Json.IsActiveState()).Json;
        }

        /// <summary>
        /// Base class deletes by Item.Id, this method deletes by Item.Json.Id
        /// </summary>
        /// <param name="id">TesTask Id</param>
        /// <returns></returns>
        /// <param name="cancellationToken"></param>
        public async Task DeleteItemAsync(string id, CancellationToken cancellationToken)
        {
            _ = await AddUpdateOrRemoveItemInDbAsync(await GetItemFromCacheOrDatabase(id, true, cancellationToken), WriteAction.Delete, cancellationToken);
            _ = Cache?.TryRemove(id);
        }

        /// <summary>
        /// Identical to GetItemsAsync, paging is not supported. All items are returned
        /// </summary>
        /// <param name="predicate">Predicate to query the TesTasks</param>
        /// <param name="pageSize"></param>
        /// <param name="continuationToken"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<(string, IEnumerable<TesTask>)> GetItemsAsync(Expression<Func<TesTask, bool>> predicate, int pageSize, string continuationToken, CancellationToken cancellationToken)
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
            var results = (await InternalGetItemsAsync(predicate, cancellationToken, pagination: q => q.Where(t => t.Json.Id.CompareTo(last.Id) > 0).Take(pageSize))).ToList();

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

        /// <inheritdoc/>
        public ValueTask<bool> TryRemoveItemFromCacheAsync(TesTask item, CancellationToken _1)
        {
            return ValueTask.FromResult(Cache?.TryRemove(item.Id) ?? false);
        }
    }
}
