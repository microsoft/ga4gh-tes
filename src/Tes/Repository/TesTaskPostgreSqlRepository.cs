// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Repository
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading.Tasks;
    using Microsoft.EntityFrameworkCore;
    using Tes.Models;

    /// <summary>
    /// A TesTask specific repository for CRUD-ing the JSON TesTask stored in a TesTaskDatabaseItem
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
        /// Base class searches within model, this method searches within the JSON
        /// </summary>
        /// <param name="id">The TesTask Id stored in the TesTaskDatabaseItem JSON</param>
        /// <param name="onSuccess">Delegate to run on success</param>
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
        /// Base class searches within model, this method searches within the JSON
        /// </summary>
        /// <param name="predicate">Predicate to run on the JSON</param>
        /// <returns></returns>
        public async Task<IEnumerable<TesTask>> GetItemsAsync(Expression<Func<TesTask, bool>> predicate)
        {
            using var dbContext = createDbContext();

            // TODO verify that this does not return all TesTasks and then execute the predicate locally on all items
            // Search for items in the JSON
            var query = dbContext.TesTasks.Select(t => t.Json).Where(predicate);
            var sqlQuery = query.ToQueryString();
            Debugger.Break();
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
                throw new Exception($"No TesTask with Json.Id = {tesTask.Id} found in the database.");
            }

            item.Json = tesTask;
            await dbContext.SaveChangesAsync();
            return item.Json;
        }

        /// <summary>
        /// Base class delets by Item.Id, this method deletes by Item.Json.Id
        /// </summary>
        /// <param name="id">TesTask Id</param>
        /// <returns></returns>
        public async Task DeleteItemAsync(string id)
        {
            using var dbContext = createDbContext();
            var item = await dbContext.TesTasks.FirstOrDefaultAsync(t => t.Json.Id == id);

            if (item is null)
            {
                throw new Exception($"No TesTask with Json.Id = {item.Id} found in the database.");
            }

            dbContext.TesTasks.Remove(item);
            await dbContext.SaveChangesAsync();
        }

        public Task<(string, IEnumerable<TesTask>)> GetItemsAsync(Expression<Func<TesTask, bool>> predicate, int pageSize, string continuationToken)
        {
            throw new NotImplementedException();
        }
    }
}
