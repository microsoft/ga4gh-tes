// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Repository
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading.Tasks;
    using Microsoft.EntityFrameworkCore;


    /// <summary>
    /// A repository for interacting with an Azure PostgreSql Server. 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class PostgreSqlRepository<T> : IRepository<T> where T : RepositoryItem<T>
    {
        private readonly string connectionString;

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="host">Azure PostgreSQL Server host name</param>
        /// <param name="user">Azure PostgreSQL Server user name</param>
        /// <param name="database">Azure PostgreSQL Server database name</param>
        /// <param name="token">User's password or authentication token for Azure PostgreSQL Server</param>
        public PostgreSqlRepository(string host, string user, string database, string token)
        {
            connectionString =
                String.Format(
                 "Server={0}; User Id={1}.postgres.database.azure.com; Database={2}; Port={3}; Password={4}; SSLMode=Prefer",
                 host,
                 user,
                 database,
                 5432,
                 token);
        }

        /// <inheritdoc/>
        public async Task<bool> TryGetItemAsync(string id, Action<T> onSuccess = null)
        {
            using var context = new RepositoryDb();
            await context.Database.EnsureCreatedAsync();

            // Search for Id within the JSON
            var task = await context.Items.FirstOrDefaultAsync(t => t.Json.GetId().Contains(id));

            if (task is not null)
            {
                onSuccess?.Invoke(task.Json);
                return true;
            }
            return false;
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<T>> GetItemsAsync(Expression<Func<T, bool>> predicate)
        {
            using var context = new RepositoryDb();
            await context.Database.EnsureCreatedAsync();

            // Search for items in the JSON
            return await context.Items.Select(t => t.Json).Where(predicate).ToListAsync<T>();
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
            using var context = new RepositoryDb(connectionString);
            await context.Database.EnsureCreatedAsync();
            var dbItem = new DatabaseItem { Json = item };
            context.Items.Add(dbItem);
            await context.SaveChangesAsync();
            return item;
        }

        /// <inheritdoc/>
        public async Task<T> UpdateItemAsync(T item)
        {
            using var context = new RepositoryDb();
            await context.Database.EnsureCreatedAsync();

            var task = await context.Items.FirstOrDefaultAsync(t => t.Json.GetId == item.GetId);

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
        public async Task DeleteItemAsync(string id)
        {
            using var context = new RepositoryDb();
            await context.Database.EnsureCreatedAsync();
            var task = await context.Items.FirstOrDefaultAsync(t => t.Id == (long)Convert.ToDouble(id));

            if (task is not null)
            {
                context.Items.Remove(task);
                await context.SaveChangesAsync();
            }
        }

        public class RepositoryDb : DbContext
        {
            private readonly string connectionString;
            public RepositoryDb(string connectionString = null)
            {
                this.connectionString = connectionString;
            }

            public DbSet<DatabaseItem> Items { get; set; }
            protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            {
                optionsBuilder.UseNpgsql(connectionString).UseLowerCaseNamingConvention();
            }
        }

        /// <summary>
        /// Database schema for encapsulating a RepositoryItem<T> as Json.
        /// </summary>
        [Table("databaseitem")]
        public class DatabaseItem
        {
            [Column("id")]
            public long Id { get; set; }
            [Column("json", TypeName = "jsonb")]
            public T Json { get; set; }
        }
    }
}
