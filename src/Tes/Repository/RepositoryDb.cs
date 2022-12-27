using Microsoft.EntityFrameworkCore;
using Tes.Models;

namespace Tes.Repository
{
    public class RepositoryDb : DbContext
    {
        public string connectionString;
        public RepositoryDb(string connectionString = null)
        {
            this.connectionString = connectionString;
        }

        public DbSet<TeskTaskDatabaseItem> TesTasks { get; set; }
        // TODO (bmurri): Create DbSet for Pool items
        //public DbSet<PoolDatabaseItem> PoolItems { get; set; }
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseNpgsql(connectionString).UseLowerCaseNamingConvention();
        }
    }
}
