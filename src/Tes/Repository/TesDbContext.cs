using Microsoft.EntityFrameworkCore;
using Tes.Models;

namespace Tes.Repository
{
    public class TesDbContext : DbContext
    {
        public const string TesTasksPostgresTableName = "testasks";

        public TesDbContext(string connectionString = null)
        {
            ConnectionString = connectionString;
        }

        public string ConnectionString { get; set; }
        public DbSet<TesTaskDatabaseItem> TesTasks { get; set; }
        //public DbSet<Pool> Pools { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder.UseNpgsql(ConnectionString, options => options.MaxBatchSize(1000)).UseLowerCaseNamingConvention();
            }
        }
    }
}
