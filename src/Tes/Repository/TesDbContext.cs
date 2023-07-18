// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Text;
using Azure.Identity;
using Microsoft.EntityFrameworkCore;
using Tes.Models;

namespace Tes.Repository
{
    public class TesDbContext : DbContext
    {
        private const string aadResourceId = "https://ossrdbms-aad.database.windows.net/.default";
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

        protected override async void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                if (ConnectionString.Contains("PASSWORD=CLIENT_ID;", StringComparison.OrdinalIgnoreCase))
                {
                    // Use AAD managed identity (https://learn.microsoft.com/en-us/azure/postgresql/single-server/how-to-connect-with-managed-identity)
                    // Instructions:
                    // 1.  You must run this on your PostgreSQL server, replacing 'myuser' with the correct user:
                    /*
                            SET aad_validate_oids_in_tenant = off;
                            CREATE ROLE myuser WITH LOGIN PASSWORD 'CLIENT_ID' IN ROLE azure_ad_user;
                    */
                    // 2.  You must set DatabaseUserPassword to "CLIENT_ID" in the TES AKS configuration

                    // Note: this supports token caching internally
                    var credential = new DefaultAzureCredential();
                    var accessToken = await credential.GetTokenAsync(
                        new Azure.Core.TokenRequestContext(scopes: new string[] { aadResourceId }));

                    ConnectionString.Replace("PASSWORD=CLIENT_ID;", $"PASSWORD={accessToken.Token};");
                }

                optionsBuilder
                    .UseNpgsql(ConnectionString, options => options.MaxBatchSize(1000))
                    .UseLowerCaseNamingConvention();
            }
        }
    }
}
