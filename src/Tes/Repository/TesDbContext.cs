using Microsoft.EntityFrameworkCore;
using Tes.Models;

namespace Tes.Repository
{
    public class TesDbContext : DbContext
    {
        public const string TesTasksPostgresTableName = "testasks";
        public string ConnectionString { get; set; }

        public TesDbContext(string connectionString = null)
        {
            ConnectionString = connectionString;
        }

        public DbSet<TesTaskDatabaseItem> TesTasks { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseNpgsql(ConnectionString).UseLowerCaseNamingConvention();
        }
    }
}
