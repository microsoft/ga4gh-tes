using System;
using Microsoft.EntityFrameworkCore;
using Tes.Models;

namespace Tes.Repository
{
    public class TesDbContext : DbContext
    {
        public const string TesTasksPostgresTableName = "testasks";

        public TesDbContext()
        {
            // default constructor for EF migration generation
        }

        public TesDbContext(string connectionString)
        {
            ArgumentException.ThrowIfNullOrEmpty(connectionString, nameof(connectionString));
            ConnectionString = connectionString;
        }

        public string ConnectionString { get; set; }
        public DbSet<TesTaskDatabaseItem> TesTasks { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                // use PostgreSQL
                optionsBuilder
                    .UseNpgsql(ConnectionString, options => options.MaxBatchSize(1000))
                    .UseLowerCaseNamingConvention();
            }
        }

        // Adds a GIN index for fast queries within the JSON
        // docs: https://www.npgsql.org/efcore/modeling/indexes.html#index-methods
        // https://www.postgresql.org/docs/current/gin-intro.html
        // Equivalent of: CREATE INDEX ix_testasks_json ON testasks USING gin (json);
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<TesTaskDatabaseItem>()
                .HasIndex(b => new { b.Json })
                .HasMethod("gin");
    }
}
