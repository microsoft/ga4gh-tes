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



    /// <summary>
    /// A repository for interacting with an Azure PostgreSql Server. 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class GenericPostgreSqlRepository<T> : IRepository<T> where T : RepositoryItem<T>
    {
        protected readonly Func<RepositoryDb> createDbContext;

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="createDbContext">A delegate that creates a RepositoryDb context</param>
        public GenericPostgreSqlRepository(Func<RepositoryDb> createDbContext)
        {
            this.createDbContext = createDbContext;
        }

        /// <inheritdoc/>
        public async Task<bool> TryGetItemAsync(string id, Action<T> onSuccess = null)
        {
            using var dbContext = createDbContext();
            // Search for Id in the Set (Which would the outer model, not the JSON)
            var task = await dbContext.Set<T>().FirstOrDefaultAsync(t => t.GetId() == id);

            if (task is not null)
            {
                onSuccess?.Invoke(task);
                return true;
            }
            return false;
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<T>> GetItemsAsync(Expression<Func<T, bool>> predicate)
        {
            using var dbContext = createDbContext();
            // Search for items in the outer model, not the JSON
            return await dbContext.Set<T>().Where(predicate).ToListAsync<T>();
        }

        /// <inheritdoc/>
        public async Task<(string, IEnumerable<T>)> GetItemsAsync(Expression<Func<T, bool>> predicate, int pageSize, string continuationToken)
        {
            // Paging in PostgreSql is inefficient and should not be implemented unless absolutely necessary.
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public async Task<T> CreateItemAsync(T item)
        {
            using var dbContext = createDbContext();
            dbContext.Set<T>().Add(item);
            await dbContext.SaveChangesAsync();
            return item;
        }

        /// <inheritdoc/>
        public async Task<T> UpdateItemAsync(T item)
        {
            using var dbContext = createDbContext();

            // Get outer model Id
            var task = await dbContext.Set<T>().FirstOrDefaultAsync(t => t.GetId() == item.GetId());

            // Update Properties
            if (task is not null)
            {
                task = item;
                await dbContext.SaveChangesAsync();
                return task;
            }
            return null;
        }

        /// <inheritdoc/>
        public async Task DeleteItemAsync(string id)
        {
            using var dbContext = createDbContext();
            // Searches outer model, not JSON
            var task = await dbContext.Set<T>().FirstOrDefaultAsync(t => t.GetId() == id);

            if (task is not null)
            {
                dbContext.Set<T>().Remove(task);
                await dbContext.SaveChangesAsync();
            }
        }
    }
}
