// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Repository
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text.Json;
    using System.Text.Json.Serialization;
    using System.Text.Json.Serialization.Metadata;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Options;
    using Npgsql;
    using Tes.Models;
    using Tes.Repository.Models;
    using Tes.Repository.Utilities;
    using Tes.Utilities;

    /// <summary>
    /// A TesTask specific repository for storing the TesTask as JSON within an Entity Framework Postgres table
    /// </summary>
    /// <typeparam name="TesTask"></typeparam>
    public sealed class TesTaskPostgreSqlRepository : PostgreSqlCachingRepository<TesTaskDatabaseItem, TesTask>, IRepository<TesTask>
    {
        // JsonSerializerOptions singleton factory
        private static readonly Lazy<JsonSerializerOptions> GetSerializerOptions = new(() =>
        {
            // Create, configure and return JsonSerializerOptions.
            JsonSerializerOptions options = new(JsonSerializerOptions.Default)
            {
                // Be somewhat minimalistic when serializing. Non-null default property values (for any given type) are still written.
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,

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
                    typeInfo.UnmappedMemberHandling ??= JsonUnmappedMemberHandling.Skip;
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
        public static Func<string, NpgsqlDataSource> NpgsqlDataSourceFunc => connectionString =>
            new NpgsqlDataSourceBuilder(connectionString)
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
            Logger.LogWarning(@"TesTaskPostgreSqlRepository::GetItemsAsync called"); // TODO: remove this log
            return (await InternalGetItemsAsync(cancellationToken, efPredicates: [predicate])).Select(t => t.TesTask);
        }

        /// <inheritdoc/>
        public async Task<TesTask> CreateItemAsync(TesTask task, CancellationToken cancellationToken)
        {
            var item = new TesTaskDatabaseItem { Json = task };
            item = await ExecuteNpgsqlActionAsync(async () => await AddUpdateOrRemoveItemInDbAsync(item, db => db.Json, WriteAction.Add, cancellationToken));
            return EnsureActiveItemInCache(item, t => t.Json.Id, t => t.Json.IsActiveState(), CopyTesTask).TesTask;
        }

        /// <summary>
        /// Encapsulates TesTasks as JSON
        /// </summary>
        /// <param name="item">TesTask to store as JSON in the database</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public async Task<List<TesTask>> CreateItemsAsync(List<TesTask> items, CancellationToken cancellationToken)
             => [.. (await Task.WhenAll(items.Select(task => CreateItemAsync(task, cancellationToken))))];

        /// <inheritdoc/>
        public async Task<TesTask> UpdateItemAsync(TesTask tesTask, CancellationToken cancellationToken)
        {
            var item = await ExecuteNpgsqlActionAsync(async () => await GetItemFromCacheOrDatabase(tesTask.Id, true, cancellationToken));
            item.Json = tesTask;
            item = await ExecuteNpgsqlActionAsync(async () => await AddUpdateOrRemoveItemInDbAsync(item, db => db.Json, WriteAction.Update, cancellationToken));
            return EnsureActiveItemInCache(item, t => t.Json.Id, t => t.Json.IsActiveState(), CopyTesTask).TesTask;
        }

        /// <inheritdoc/>
        public async Task DeleteItemAsync(string id, CancellationToken cancellationToken)
        {
            _ = await ExecuteNpgsqlActionAsync(async () => await AddUpdateOrRemoveItemInDbAsync(await GetItemFromCacheOrDatabase(id, true, cancellationToken), db => db.Json, WriteAction.Delete, cancellationToken));
            _ = Cache?.TryRemove(id);
        }

        internal record struct ContinuationToken(long LastDbId, string Raw, string EF)
        {
            private const char EfSeparator = '&';

            internal static ContinuationToken GetToken(FormattableString RawQuery, IEnumerable<Expression<Func<TesTask, bool>>> EfQuery, long? Id = null)
            {
                return new(
                    Id ?? long.MinValue,
                    RawQuery?.ToString(System.Globalization.CultureInfo.InvariantCulture),
                    EfQuery is null ? null : string.Join(EfSeparator, EfQuery.Select(p => System.Web.HttpUtility.UrlEncode(p.ToString()))));
            }

            internal readonly string AsString(long? lastId)
            {
                return lastId is null
                    ? null
                    : Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new(lastId.Value, Raw, EF), SourceGenerationContext.Default.ContinuationToken)));
            }

            internal readonly ContinuationToken Parse(string previous)
            {
                if (string.IsNullOrWhiteSpace(previous))
                {
                    return this;
                }

                var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent((int)Math.Ceiling(previous.Length / 4.0) * 3);

                try
                {
                    if (Convert.TryFromBase64String(previous, buffer, out var bytesWritten))
                    {
                        var token = JsonSerializer.Deserialize(buffer.AsSpan()[..bytesWritten], SourceGenerationContext.Default.ContinuationToken);

                        if (!(Raw ?? string.Empty).Equals((token.Raw ?? string.Empty), StringComparison.Ordinal) || !(EF ?? string.Empty).Equals((token.EF ?? string.Empty), StringComparison.Ordinal))
                        {
                            throw new ArgumentException("Invalid query parameters for page_token.", nameof(previous));
                        }

                        return token;
                    }
                    else
                    {
                        throw new ArgumentException("page_token is corrupt or invalid.", nameof(previous));
                    }
                }
                catch (NotSupportedException ex)
                {
                    throw new ArgumentException("page_token is corrupt or invalid.", nameof(previous), ex);
                }
                catch (JsonException ex)
                {
                    throw new ArgumentException("page_token is corrupt or invalid.", nameof(previous), ex);
                }
                finally
                {
                    System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
                }
            }
        }

        /// <inheritdoc/>
        public async Task<IRepository<TesTask>.GetItemsResult> GetItemsAsync(string continuationToken, int pageSize, CancellationToken cancellationToken, FormattableString rawPredicate, IEnumerable<Expression<Func<TesTask, bool>>> efPredicates)
        {
            var token = ContinuationToken.GetToken(rawPredicate, efPredicates).Parse(continuationToken);

            var results = (await InternalGetItemsAsync(
                    cancellationToken,
                    pagination: q => q.Where(t => t.Id > token.LastDbId).Take(pageSize),
                    orderBy: q => q.OrderBy(t => t.Id),
                    efPredicates: efPredicates,
                    rawPredicate: rawPredicate))
                .ToList();

            return new(
                token.AsString(results.Count == pageSize ? results.LastOrDefault()?.DbId : null),
                results.Select(t => t.TesTask));
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
                item = await ExecuteNpgsqlActionAsync(async () => await asyncPolicy.ExecuteWithRetryAsync(ct => dbContext.TesTasks.FirstOrDefaultAsync(t => t.Json.Id == id, ct), cancellationToken));

                if (throwIfNotFound && item is null)
                {
                    throw new KeyNotFoundException($"No TesTask with ID {id} found in the database.");
                }
            }

            return item.Clone();
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
        private async Task<IEnumerable<GetItemsResult>> InternalGetItemsAsync(CancellationToken cancellationToken, Func<IQueryable<TesTaskDatabaseItem>, IQueryable<TesTaskDatabaseItem>> orderBy = default, Func<IQueryable<TesTaskDatabaseItem>, IQueryable<TesTaskDatabaseItem>> pagination = default, IEnumerable<Expression<Func<TesTask, bool>>> efPredicates = default, FormattableString rawPredicate = default)
        {
            using var dbContext = CreateDbContext();
            return await ExecuteNpgsqlActionAsync(async () =>
                (await GetItemsAsync(dbContext.TesTasks, cancellationToken, orderBy, pagination, efPredicates?.Select(WhereTesTask), rawPredicate))
                    .Select(item => EnsureActiveItemInCache(item, t => t.Json.Id, t => t.Json.IsActiveState(), CopyTesTask)));
        }

        private async Task<T> ExecuteNpgsqlActionAsync<T>(Func<Task<T>> action)
        {
            try
            {
                return await action();
            }
            catch (NpgsqlException npgEx) when (npgEx.InnerException is TimeoutException)
            {
                Logger?.LogError(npgEx, "PostgreSQL: {NpgsqlTimeoutMessage}", npgEx.Message);
                throw LogDatabaseOverloadedException(npgEx);
            }
            catch (InvalidOperationException ioEx) when (ioEx.InnerException is TimeoutException)
            {
                Logger?.LogError(ioEx, "PostgreSQL: {InvalidOpTimeoutMessage}", ioEx.Message);
                throw LogDatabaseOverloadedException(ioEx);
            }
            catch (InvalidOperationException ioEx) when
                (ioEx.InnerException is NpgsqlException npgSqlEx
                && npgSqlEx.Message?.StartsWith("The connection pool has been exhausted", StringComparison.OrdinalIgnoreCase) == true)
            {
                Logger?.LogError(ioEx, "PostgreSQL: {InvalidOpNpgsqMessage}", ioEx.Message);
                throw LogDatabaseOverloadedException(ioEx);
            }
        }

        public DatabaseOverloadedException LogDatabaseOverloadedException(Exception exception)
        {
            var dbException = new DatabaseOverloadedException(exception);
            Logger?.LogCritical(dbException, "PostgreSQL: {FailureMessage}", dbException.Message);
            return dbException;
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

    [JsonSourceGenerationOptions(WriteIndented = false, GenerationMode = JsonSourceGenerationMode.Default)]
    [JsonSerializable(typeof(TesTaskPostgreSqlRepository.ContinuationToken))]
    internal partial class SourceGenerationContext : JsonSerializerContext
    { }
}
