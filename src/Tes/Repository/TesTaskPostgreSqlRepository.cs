// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Repository
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading.Tasks;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Options;
    using Npgsql;
    using Tes.Models;
    using Tes.Utilities;

    /// <summary>
    /// A TesTask specific repository for storing the TesTask as JSON within an Entity Framework Postgres table
    /// </summary>
    /// <typeparam name="TesTask"></typeparam>
    public class TesTaskPostgreSqlRepository : IRepository<TesTask>
    {
        private readonly Func<TesDbContext> createDbContext;
        private readonly ILogger logger;

        /// <summary>
        /// Default constructor that also will create the schema if it does not exist
        /// </summary>
        /// <param name="connectionString">The PostgreSql connection string</param>
        public TesTaskPostgreSqlRepository(string connectionString, ILogger logger)
        {
            this.logger = logger;
            createDbContext = () => { return new TesDbContext(connectionString); };
            using var dbContext = createDbContext();
            dbContext.Database.EnsureCreatedAsync().Wait();
        }

        /// <summary>
        /// Default constructor that also will create the schema if it does not exist
        /// </summary>
        /// <param name="connectionString">The PostgreSql connection string</param>
        public TesTaskPostgreSqlRepository(IOptions<PostgreSqlOptions> options, ILogger logger)
        {
            this.logger = logger;
            var connectionString = new ConnectionStringUtility().GetPostgresConnectionString(options);
            createDbContext = () => { return new TesDbContext(connectionString); };
            using var dbContext = createDbContext();
            dbContext.Database.EnsureCreatedAsync().Wait();
        }

        /// <summary>
        /// Constructor for testing to enable mocking DbContext 
        /// </summary>
        /// <param name="createDbContext">A delegate that creates a TesTaskPostgreSqlRepository context</param>
        public TesTaskPostgreSqlRepository(Func<TesDbContext> createDbContext, ILogger logger)
        {
            this.logger = logger;
            this.createDbContext = createDbContext;
            using var dbContext = createDbContext();
            dbContext.Database.EnsureCreatedAsync().Wait();
        }

        /// <summary>
        /// Get a TesTask by the TesTask.ID
        /// </summary>
        /// <param name="id">The TesTask's ID</param>
        /// <param name="onSuccess">Delegate to be invoked on success</param>
        /// <returns></returns>
        public async Task<bool> TryGetItemAsync(string id, Action<TesTask> onSuccess = null)
        {
            return await ExecuteAsync(async dbContext =>
            {
                // Search for Id within the JSON
                var item = await dbContext.TesTasks.FirstOrDefaultAsync(t => t.Json.Id == id);

                if (item is null)
                {
                    return false;
                }

                onSuccess?.Invoke(item.Json);
                return true;
            });
        }

        /// <summary>
        /// Get TesTask items
        /// </summary>
        /// <param name="predicate">Predicate to query the TesTasks</param>
        /// <returns></returns>
        public async Task<IEnumerable<TesTask>> GetItemsAsync(Expression<Func<TesTask, bool>> predicate)
        {
            return await ExecuteAsync(async dbContext =>
            {
                // Search for items in the JSON
                var query = dbContext.TesTasks.Select(t => t.Json).Where(predicate);

                //var sqlQuery = query.ToQueryString();
                //Debugger.Break();
                return await query.ToListAsync();
            });
        }

        /// <summary>
        /// Encapsulates a TesTask as JSON
        /// </summary>
        /// <param name="item">TesTask to store as JSON in the database</param>
        /// <returns></returns>
        public async Task<TesTask> CreateItemAsync(TesTask item)
        {
            return await ExecuteAsync(async dbContext =>
            {
                var dbItem = new TesTaskDatabaseItem { Json = item };
                dbContext.TesTasks.Add(dbItem);
                await dbContext.SaveChangesAsync();
                return item;
            });
        }

        /// <summary>
        /// Encapsulates TesTasks as JSON
        /// </summary>
        /// <param name="item">TesTask to store as JSON in the database</param>
        /// <returns></returns>
        public async Task<List<TesTask>> CreateItemsAsync(List<TesTask> items)
        {
            return await ExecuteAsync(async dbContext =>
            {
                foreach (var item in items)
                {
                    var dbItem = new TesTaskDatabaseItem { Json = item };
                    dbContext.TesTasks.Add(dbItem);
                }

                await dbContext.SaveChangesAsync();

                return items;
            });
        }

        /// <summary>
        /// Base class searches within model, this method searches within the JSON
        /// </summary>
        /// <param name="tesTask"></param>
        /// <returns></returns>
        public async Task<TesTask> UpdateItemAsync(TesTask tesTask)
        {
            return await ExecuteAsync(async dbContext =>
            {
                // Manually set entity state to avoid potential NPG PostgreSql bug
                dbContext.ChangeTracker.AutoDetectChangesEnabled = false;
                var item = await dbContext.TesTasks.FirstOrDefaultAsync(t => t.Json.Id == tesTask.Id);

                if (item is null)
                {
                    throw new Exception($"No TesTask with ID {tesTask.Id} found in the database.");
                }

                item.Json = tesTask;

                // Manually set entity state to avoid potential NPG PostgreSql bug
                dbContext.Entry(item).State = EntityState.Modified;
                await dbContext.SaveChangesAsync();
                return item.Json;
            });
        }

        /// <summary>
        /// Base class deletes by Item.Id, this method deletes by Item.Json.Id
        /// </summary>
        /// <param name="id">TesTask Id</param>
        /// <returns></returns>
        public async Task DeleteItemAsync(string id)
        {
            await ExecuteAsync(async dbContext =>
            {
                var item = await dbContext.TesTasks.FirstOrDefaultAsync(t => t.Json.Id == id);

                if (item is null)
                {
                    throw new Exception($"No TesTask with ID {item.Id} found in the database.");
                }

                dbContext.TesTasks.Remove(item);
                await dbContext.SaveChangesAsync();
            });
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
            // TODO paging support
            return (null, await GetItemsAsync(predicate));
        }

        private async Task<T> ExecuteAsync<T>(Func<TesDbContext, Task<T>> action)
        {
            try
            {
                using var dbContext = createDbContext();
                return await action(dbContext);
            }
            catch (NpgsqlException npgEx) when (npgEx.InnerException is TimeoutException)
            {
                throw LogAndThrowDatabaseOverloadedException();
            }
            catch (InvalidOperationException ioEx) when (ioEx.InnerException is TimeoutException)
            {
                throw LogAndThrowDatabaseOverloadedException();
            }
            catch (InvalidOperationException ioEx) when
                (ioEx.InnerException is NpgsqlException npgSqlEx
                && npgSqlEx.Message?.StartsWith("The connection pool has been exhausted", StringComparison.OrdinalIgnoreCase) == true)
            {
                throw LogAndThrowDatabaseOverloadedException();
            }
        }

        private async Task ExecuteAsync(Func<TesDbContext, Task> action)
        {
            try
            {
                using var dbContext = createDbContext();
                await action(dbContext);
            }
            catch (NpgsqlException npgEx) when (npgEx.InnerException is TimeoutException)
            {
                throw LogAndThrowDatabaseOverloadedException();
            }
            catch (InvalidOperationException ioEx) when (ioEx.InnerException is TimeoutException)
            {
                throw LogAndThrowDatabaseOverloadedException();
            }
            catch (InvalidOperationException ioEx) when
                (ioEx.InnerException is NpgsqlException npgSqlEx
                && npgSqlEx.Message?.StartsWith("The connection pool has been exhausted", StringComparison.OrdinalIgnoreCase) == true)
            {
                throw LogAndThrowDatabaseOverloadedException();
            }
        }

        public DatabaseOverloadedException LogAndThrowDatabaseOverloadedException()
        {
            var exception = new DatabaseOverloadedException();
            logger.LogCritical(exception, exception.Message);
            return exception;
        }

        public void Dispose()
        {

        }
    }
}
