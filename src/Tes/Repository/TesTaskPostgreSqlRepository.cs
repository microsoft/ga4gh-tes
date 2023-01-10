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
    using Tes.Models;

    /// <summary>
    /// A TesTask specific repository for storing the TesTask as JSON within an Entity Framework Postgres table
    /// </summary>
    /// <typeparam name="TesTask"></typeparam>
    public class TesTaskPostgreSqlRepository : IRepository<TesTask>
    {
        private readonly Func<TesDbContext> createDbContext;

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="connectionString">The PostgreSql connection string</param>
        public TesTaskPostgreSqlRepository(string connectionString)
        {
            createDbContext = () => { return new TesDbContext(connectionString); };
            using var dbContext = createDbContext();
            dbContext.Database.EnsureCreatedAsync().Wait();
        }

        /// <summary>
        /// Constructor for testing to enable mocking DbContext 
        /// </summary>
        /// <param name="createDbContext">A delegate that creates a TesTaskPostgreSqlRepository context</param>
        public TesTaskPostgreSqlRepository(Func<TesDbContext> createDbContext)
        {
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
            using var dbContext = createDbContext();

            // Search for Id within the JSON
            var item = await dbContext.TesTasks.FirstOrDefaultAsync(t => t.Json.Id == id);

            if (item is null)
            {
                return false;
            }

            onSuccess?.Invoke(item.Json);
            return true;
        }

        /// <summary>
        /// Get TesTask items
        /// </summary>
        /// <param name="predicate">Predicate to query the TesTasks</param>
        /// <returns></returns>
        public async Task<IEnumerable<TesTask>> GetItemsAsync(Expression<Func<TesTask, bool>> predicate)
        {
            using var dbContext = createDbContext();

            // Search for items in the JSON
            var query = dbContext.TesTasks.Select(t => t.Json).Where(predicate);
            
            //var sqlQuery = query.ToQueryString();
            //Debugger.Break();
            return await query.ToListAsync();
        }

        /// <summary>
        /// Encapsulates a TesTask as JSON
        /// </summary>
        /// <param name="item">TesTask to store as JSON in the database</param>
        /// <returns></returns>
        public async Task<TesTask> CreateItemAsync(TesTask item)
        {
            using var dbContext = createDbContext();
            var dbItem = new TesTaskDatabaseItem { Json = item };
            dbContext.TesTasks.Add(dbItem);
            await dbContext.SaveChangesAsync();
            return item;
        }

        /// <summary>
        /// Encapsulates TesTasks as JSON
        /// </summary>
        /// <param name="item">TesTask to store as JSON in the database</param>
        /// <returns></returns>
        public async Task<List<TesTask>> CreateItemsAsync(List<TesTask> items)
        {
            using var dbContext = createDbContext();

            foreach (var item in items) 
            {
                var dbItem = new TesTaskDatabaseItem { Json = item };
                dbContext.TesTasks.Add(dbItem);
            }

            await dbContext.SaveChangesAsync();
            return items;
        }

        /// <summary>
        /// Base class searches within model, this method searches within the JSON
        /// </summary>
        /// <param name="tesTask"></param>
        /// <returns></returns>
        public async Task<TesTask> UpdateItemAsync(TesTask tesTask)
        {
            using var dbContext = createDbContext();
            var item = await dbContext.TesTasks.FirstOrDefaultAsync(t => t.Json.Id == tesTask.Id);

            if (item is null)
            {
                throw new Exception($"No TesTask with ID {tesTask.Id} found in the database.");
            }

            item.Json = tesTask;
            await dbContext.SaveChangesAsync();
            return item.Json;
        }

        /// <summary>
        /// Base class deletes by Item.Id, this method deletes by Item.Json.Id
        /// </summary>
        /// <param name="id">TesTask Id</param>
        /// <returns></returns>
        public async Task DeleteItemAsync(string id)
        {
            using var dbContext = createDbContext();
            var item = await dbContext.TesTasks.FirstOrDefaultAsync(t => t.Json.Id == id);

            if (item is null)
            {
                throw new Exception($"No TesTask with ID {item.Id} found in the database.");
            }

            dbContext.TesTasks.Remove(item);
            await dbContext.SaveChangesAsync();
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
            var results = await GetItemsAsync(predicate);
            return (null, results);
        }
    }
}
