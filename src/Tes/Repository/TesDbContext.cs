// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Tes.Models;

namespace Tes.Repository
{
    public class TesDbContext : DbContext
    {
        public const string TesTasksPostgresTableName = "testasks";

        public TesDbContext()
        {
            // Default constructor, which is required to run the EF migrations tool,
            // "dotnet ef migrations add InitialCreate"
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
                    .UseNpgsql(new NpgsqlDataSourceBuilder(ConnectionString)
                            .EnableDynamicJson(jsonbClrTypes: new[] { typeof(TesTask) })
                            .Build(),
                        options => options.MaxBatchSize(1000))
                    .UseLowerCaseNamingConvention();
            }
        }
    }
}
