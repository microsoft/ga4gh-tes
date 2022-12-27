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
        protected readonly RepositoryDb context;

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="createDbContext">A delegate that creates a RepositoryDb context</param>
        public GenericPostgreSqlRepository(Func<RepositoryDb> createDbContext)
        {
            this.context = createDbContext.Invoke();
        }

        /// <inheritdoc/>
        public async Task<bool> TryGetItemAsync(string id, Action<T> onSuccess = null)
        {
            await context.Database.EnsureCreatedAsync();
            // Search for Id in the Set (Which would the outer model, not the JSON)
            var task = await context.Set<T>().FirstOrDefaultAsync(t => t.GetId() == id);

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
            await context.Database.EnsureCreatedAsync();
            // Search for items in the outer model, not the JSON
            return await context.Set<T>().Where(predicate).ToListAsync<T>();
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
            await context.Database.EnsureCreatedAsync();
            context.Set<T>().Add(item);
            await context.SaveChangesAsync();
            return item;
        }

        /// <inheritdoc/>
        public async Task<T> UpdateItemAsync(T item)
        {
            await context.Database.EnsureCreatedAsync();

            // Get outer model Id
            var task = await context.Set<T>().FirstOrDefaultAsync(t => t.GetId() == item.GetId());

            // Update Properties
            if (task is not null)
            {
                task = item;
                await context.SaveChangesAsync();
                return task;
            }
            return null;
        }

        /// <inheritdoc/>
        public async Task DeleteItemAsync(string id)
        {
            await context.Database.EnsureCreatedAsync();
            // Searches outer model, not JSON
            var task = await context.Set<T>().FirstOrDefaultAsync(t => t.GetId() == id);

            if (task is not null)
            {
                context.Set<T>().Remove(task);
                await context.SaveChangesAsync();
            }
        }
    }
}
