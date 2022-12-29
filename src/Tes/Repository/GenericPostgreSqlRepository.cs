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
        public virtual async Task<bool> TryGetItemAsync(string id, Action<T> onSuccess = null)
        {
            using var dbContext = createDbContext();
            // Search for Id in the Set (Which would the outer model, not the JSON)
            var item = await dbContext.Set<T>().FirstOrDefaultAsync(t => t.GetId() == id);

            if (item is not null)
            {
                onSuccess?.Invoke(item);
                return true;
            }
            return false;
        }

        /// <inheritdoc/>
        public virtual async Task<IEnumerable<T>> GetItemsAsync(Expression<Func<T, bool>> predicate)
        {
            using var dbContext = createDbContext();
            // Search for items in the outer model, not the JSON
            return await dbContext.Set<T>().Where(predicate).ToListAsync<T>();
        }

        /// <inheritdoc/>
        public virtual async Task<(string, IEnumerable<T>)> GetItemsAsync(Expression<Func<T, bool>> predicate, int pageSize, string continuationToken)
        {
            // Paging in PostgreSql is inefficient and should not be implemented unless absolutely necessary.
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public virtual async Task<T> CreateItemAsync(T item)
        {
            using var dbContext = createDbContext();
            dbContext.Set<T>().Add(item);
            await dbContext.SaveChangesAsync();
            return item;
        }

        /// <inheritdoc/>
        public virtual async Task<T> UpdateItemAsync(T item)
        {
            using var dbContext = createDbContext();

            // Get outer model Id
            var dbItem = await dbContext.Set<T>().FirstOrDefaultAsync(t => t.GetId() == item.GetId());

            // Update Properties
            if (dbItem is not null)
            {
                dbItem = item;
                await dbContext.SaveChangesAsync();
                return dbItem;
            }
            return null;
        }

        /// <inheritdoc/>
        public virtual async Task DeleteItemAsync(string id)
        {
            using var dbContext = createDbContext();
            // Searches outer model, not JSON
            var item = await dbContext.Set<T>().FirstOrDefaultAsync(t => t.GetId() == id);

            if (item is not null)
            {
                dbContext.Set<T>().Remove(item);
                await dbContext.SaveChangesAsync();
            }
        }
    }
}
