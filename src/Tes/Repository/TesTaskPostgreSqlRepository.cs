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
    /// A TesTask specific repository for CRUD-ing the JSON TesTask stored in a TeskTaskDatabaseItem.
    /// </summary>
    /// <typeparam name="TesTask"></typeparam>
    public class TesTaskPostgreSqlRepository : GenericPostgreSqlRepository<TesTask>
    {
        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="createDbContext">A delegate that creates a RepositoryDb context</param>
        public TesTaskPostgreSqlRepository(Func<RepositoryDb> createDbContext) : base(createDbContext)
        {
        }

        /// <summary>
        /// Base class searches within model, this method searches within the JSON
        /// </summary>
        /// <param name="id">The TesTask Id stored in the TesTaskDatabaseItem JSON</param>
        /// <param name="onSuccess">Delegate to run on success</param>
        /// <returns></returns>
        public new async Task<bool> TryGetItemAsync(string id, Action<TesTask> onSuccess = null)
        {
            using var dbContext = createDbContext();

            // Search for Id within the JSON
            var item = await dbContext.TesTasks.FirstOrDefaultAsync(t => t.Json.GetId().Contains(id));

            if (item is not null)
            {
                onSuccess?.Invoke(item.Json);
                return true;
            }
            return false;
        }

        /// <summary>
        /// Base class searches within model, this method searches within the JSON
        /// </summary>
        /// <param name="predicate">Predicate to run on the JSON</param>
        /// <returns></returns>
        public new async Task<IEnumerable<TesTask>> GetItemsAsync(Expression<Func<TesTask, bool>> predicate)
        {
            using var dbContext = createDbContext();

            // Search for items in the JSON
            return await dbContext.TesTasks.Select(t => t.Json).Where(predicate).ToListAsync<TesTask>();
        }

        /// <summary>
        /// Encapsulates a TesTask as JSON
        /// </summary>
        /// <param name="item">TesTask to store as JSON in the database</param>
        /// <returns></returns>
        public new async Task<TesTask> CreateItemAsync(TesTask item)
        {
            using var dbContext = createDbContext();
            var dbItem = new TeskTaskDatabaseItem { Json = item };
            dbContext.TesTasks.Add(dbItem);
            await dbContext.SaveChangesAsync();
            return item;
        }

        /// <summary>
        /// Base class searches within model, this method searches within the JSON
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public new async Task<TesTask> UpdateItemAsync(TesTask item)
        {
            using var dbContext = createDbContext();
            var dbItem = await dbContext.TesTasks.FirstOrDefaultAsync(t => t.Json.GetId == item.GetId);

            // Update Properties
            if (dbItem is not null)
            {
                dbItem.Json = item;
                await dbContext.SaveChangesAsync();
                return dbItem.Json;
            }
            return null;
        }

        /// <summary>
        /// Base class deletes by TesTaskDatabaseItem Id, this allows deleting by TesTask Id.
        /// </summary>
        /// <param name="id">TesTask Id</param>
        /// <returns></returns>
        public new async Task DeleteItemAsync(string id)
        {
            using var dbContext = createDbContext();
            var item = await dbContext.TesTasks.FirstOrDefaultAsync(t => t.Json.Id == id);

            if (item is not null)
            {
                dbContext.TesTasks.Remove(item);
                await dbContext.SaveChangesAsync();
            }
        }
    }
}
