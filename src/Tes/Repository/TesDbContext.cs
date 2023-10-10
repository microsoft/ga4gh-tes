// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using Azure.Identity;
using Microsoft.EntityFrameworkCore;
using Tes.Models;
using Tes.Utilities;

namespace Tes.Repository
{
    public class TesDbContext : DbContext
    {
        private const int maxBatchSize = 1000;
        private readonly PostgresConnectionStringUtility connectionStringUtility = null!;

        public TesDbContext()
        {
            // Default constructor, which is required to run the EF migrations tool,
            // "dotnet ef migrations add InitialCreate"
            // DI will NOT use this constructor
        }

        public TesDbContext(PostgresConnectionStringUtility connectionStringUtility)
        {
            this.connectionStringUtility = connectionStringUtility;
        }

        public DbSet<TesTaskDatabaseItem> TesTasks { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            string connectionString = this.connectionStringUtility.GetConnectionString().Result;

            optionsBuilder
                .UseNpgsql(connectionString, options => options.MaxBatchSize(maxBatchSize))
                .UseLowerCaseNamingConvention();
        }
    }
}
