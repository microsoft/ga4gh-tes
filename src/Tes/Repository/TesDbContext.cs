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

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<TesTaskDatabaseItem>()
                .HasIndex(b => new { b.Json })
                .HasMethod("gin");
    }
}
