// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Npgsql.EntityFrameworkCore.PostgreSQL.Infrastructure;
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

        public TesDbContext(NpgsqlDataSource dataSource, Action<NpgsqlDbContextOptionsBuilder> contextOptionsBuilder = default)
        {
            ArgumentNullException.ThrowIfNull(dataSource, nameof(dataSource));
            DataSource = dataSource;
            ContextOptionsBuilder = contextOptionsBuilder;
        }

        public NpgsqlDataSource DataSource { get; set; }
        public Action<NpgsqlDbContextOptionsBuilder> ContextOptionsBuilder { get; set; }

        public DbSet<TesTaskDatabaseItem> TesTasks { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                // use PostgreSQL
                optionsBuilder
                    .UseNpgsql(DataSource, ContextOptionsBuilder)
                    .UseLowerCaseNamingConvention();
            }
        }
    }
}
