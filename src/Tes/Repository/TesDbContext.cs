// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using Azure.Identity;
using Microsoft.EntityFrameworkCore;
using Tes.Models;

namespace Tes.Repository
{
    public class TesDbContext : DbContext
    {
        private const string azureDatabaseForPostgresqlScope = "https://ossrdbms-aad.database.windows.net/.default";
        public const string TesTasksPostgresTableName = "testasks";
        public bool UseManagedIdentity { get; set; }

        public TesDbContext()
        {
            // Default constructor, which is required to run the EF migrations tool,
            // "dotnet ef migrations add InitialCreate"
        }

        public TesDbContext(string connectionString, bool useManagedIdentity = false)
        {
            ArgumentException.ThrowIfNullOrEmpty(connectionString, nameof(connectionString));
            ConnectionString = connectionString;
            UseManagedIdentity = useManagedIdentity;
        }

        public string ConnectionString { get; set; }
        public DbSet<TesTaskDatabaseItem> TesTasks { get; set; }

        protected override async void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            string tempConnectionString = ConnectionString;

            if (UseManagedIdentity)
            {
                // Use AAD managed identity
                // https://learn.microsoft.com/en-us/azure/postgresql/single-server/how-to-connect-with-managed-identity
                // https://learn.microsoft.com/en-us/azure/postgresql/single-server/concepts-azure-ad-authentication
                // Instructions:
                // 1.  Replace 'myuser' and run on your server
                /*
                        SET aad_validate_oids_in_tenant = off;
                        CREATE ROLE myuser WITH LOGIN PASSWORD 'CLIENT_ID' IN ROLE azure_ad_user;
                */
                // 2.  Set "PostgreSql.UseManagedIdentity" to "true" in the TES AKS configuration
                // 3.  Ensure the managed identity that TES runs under has the role

                // Note: this supports token caching internally
                var credential = new DefaultAzureCredential();
                var accessToken = await credential.GetTokenAsync(
                    new Azure.Core.TokenRequestContext(scopes: new string[] { azureDatabaseForPostgresqlScope }));

                // ConnectionStringUtility omits password when UseManagedIdentity is set, therefore
                // omitting this to avoid performance hit of string comparison on every creation
                // if (tempConnectionString.Contains("Password=", StringComparison.OrdinalIgnoreCase)) throw new Exception("Password shall not be provided when using managed identity");

                tempConnectionString = tempConnectionString.TrimEnd(';') + $";Password={accessToken.Token};";
            }

            optionsBuilder
                .UseNpgsql(tempConnectionString, options => options.MaxBatchSize(1000))
                .UseLowerCaseNamingConvention();
        }
    }
}
