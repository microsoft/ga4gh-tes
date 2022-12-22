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
    /// A repository for interacting with an Azure PostgreSql Server. 
    /// </summary>
    /// <typeparam name="TesTask"></typeparam>
    public class TesTaskPostgreSqlRepository : GenericPostgreSqlRepository<TesTask>
    {
        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="host">Azure PostgreSQL Server host name</param>
        /// <param name="user">Azure PostgreSQL Server user name</param>
        /// <param name="database">Azure PostgreSQL Server database name</param>
        /// <param name="token">User's password or authentication token for Azure PostgreSQL Server</param>
        public TesTaskPostgreSqlRepository(string host, string user, string database, string token) : base(host, user, database, token)
        {
        }

        /// Base class searches within model, this method searches within the JSON
        public new async Task<bool> TryGetItemAsync(string id, Action<TesTask> onSuccess = null)
        {
            await context.Database.EnsureCreatedAsync();

            // Search for Id within the JSON
            var task = await context.TesTasks.FirstOrDefaultAsync(t => t.Json.GetId().Contains(id));

            if (task is not null)
            {
                onSuccess?.Invoke(task.Json);
                return true;
            }
            return false;
        }

        /// Base class searches within model, this method searches within the JSON
        public new async Task<IEnumerable<TesTask>> GetItemsAsync(Expression<Func<TesTask, bool>> predicate)
        {
            await context.Database.EnsureCreatedAsync();

            // Search for items in the JSON
            return await context.TesTasks.Select(t => t.Json).Where(predicate).ToListAsync<TesTask>();
        }

        /// Encapsulates a TesTask as JSON
        public new async Task<TesTask> CreateItemAsync(TesTask item)
        {
            await context.Database.EnsureCreatedAsync();
            var dbItem = new TeskTaskDatabaseItem { Json = item };
            context.TesTasks.Add(dbItem);
            await context.SaveChangesAsync();
            return item;
        }

        /// Base class searches within model, this method searches within the JSON
        public new async Task<TesTask> UpdateItemAsync(TesTask item)
        {
            await context.Database.EnsureCreatedAsync();

            var task = await context.TesTasks.FirstOrDefaultAsync(t => t.Json.GetId == item.GetId);

            // Update Properties
            if (task is not null)
            {
                task.Json = item;
                await context.SaveChangesAsync();
                return task.Json;
            }
            return null;
        }

        /// <inheritdoc/>
        public new async Task DeleteItemAsync(string id)
        {
            await context.Database.EnsureCreatedAsync();
            var task = await context.TesTasks.FirstOrDefaultAsync(t => t.Id == (long)Convert.ToDouble(id));

            if (task is not null)
            {
                context.TesTasks.Remove(task);
                await context.SaveChangesAsync();
            }
        }
    }
}
