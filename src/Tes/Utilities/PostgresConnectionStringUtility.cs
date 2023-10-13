// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Text;
using System.Threading.Tasks;
using Azure.Core;
using Tes.Models;

namespace Tes.Utilities
{
    public class PostgresConnectionStringUtility
    {
        private const string azureDatabaseForPostgresqlScope = "https://ossrdbms-aad.database.windows.net/.default";
        private readonly string connectionString = null!;
        private readonly TokenCredential tokenCredential = null!;
        public bool UseManagedIdentity { get; set; }     

        public PostgresConnectionStringUtility(PostgreSqlOptions options, TokenCredential tokenCredential)
        {
            this.tokenCredential = tokenCredential;
            connectionString = InternalGetConnectionString(options);
            UseManagedIdentity = options.UseManagedIdentity;
        }

        public async Task<string> GetConnectionString()
        {
            if (UseManagedIdentity)
            {
                // Use AAD managed identity
                // https://learn.microsoft.com/en-us/azure/postgresql/single-server/how-to-connect-with-managed-identity
                // https://learn.microsoft.com/en-us/azure/postgresql/single-server/concepts-azure-ad-authentication

                var accessToken = await tokenCredential.GetTokenAsync(
                    new TokenRequestContext(scopes: new string[] { azureDatabaseForPostgresqlScope }), System.Threading.CancellationToken.None);

                return $"{connectionString}Password={accessToken.Token};";
            }

            return connectionString;
        }

        private string InternalGetConnectionString(PostgreSqlOptions options)
        {
            ArgumentException.ThrowIfNullOrEmpty(options.ServerName, nameof(options.ServerName));
            ArgumentException.ThrowIfNullOrEmpty(options.ServerNameSuffix, nameof(options.ServerNameSuffix));
            ArgumentException.ThrowIfNullOrEmpty(options.ServerPort, nameof(options.ServerPort));
            ArgumentException.ThrowIfNullOrEmpty(options.ServerSslMode, nameof(options.ServerSslMode));
            ArgumentException.ThrowIfNullOrEmpty(options.DatabaseName, nameof(options.DatabaseName));
            ArgumentException.ThrowIfNullOrEmpty(options.DatabaseUserLogin, nameof(options.DatabaseUserLogin));

            if (!options.UseManagedIdentity)
            {
                // Ensure password is set if NOT using Managed Identity
                ArgumentException.ThrowIfNullOrEmpty(options.DatabaseUserPassword, nameof(options.DatabaseUserPassword));
            }

            if (options.UseManagedIdentity && !string.IsNullOrWhiteSpace(options.DatabaseUserPassword))
            {
                // throw if password IS set when using Managed Identity
                throw new ArgumentException("DatabaseUserPassword shall not be set if UseManagedIdentity is true");
            }

            if (options.ServerName.Contains(options.ServerNameSuffix, StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException($"'{nameof(options.ServerName)}' should only contain the name of the server like 'myserver' and NOT the full host name like 'myserver{options.ServerNameSuffix}'", nameof(options.ServerName));
            }

            var connectionStringBuilder = new StringBuilder();
            connectionStringBuilder.Append($"Server={options.ServerName}{options.ServerNameSuffix};");
            connectionStringBuilder.Append($"Database={options.DatabaseName};");
            connectionStringBuilder.Append($"Port={options.ServerPort};");
            connectionStringBuilder.Append($"SSL Mode={options.ServerSslMode};");
            connectionStringBuilder.Append($"User Id={options.DatabaseUserLogin};");

            if (!options.UseManagedIdentity)
            {
                connectionStringBuilder.Append($"Password={options.DatabaseUserPassword};");
            }

            return connectionStringBuilder.ToString();
        }
    }
}
