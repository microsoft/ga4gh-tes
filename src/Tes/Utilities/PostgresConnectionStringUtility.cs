// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Text;
using Microsoft.Extensions.Options;
using Tes.Models;

namespace Tes.Utilities
{
    public class ConnectionStringUtility
    {
        public string GetPostgresConnectionString(IOptions<PostgreSqlOptions> options)
        {
            ArgumentException.ThrowIfNullOrEmpty(options.Value.ServerName, nameof(options.Value.ServerName));
            ArgumentException.ThrowIfNullOrEmpty(options.Value.ServerNameSuffix, nameof(options.Value.ServerNameSuffix));
            ArgumentException.ThrowIfNullOrEmpty(options.Value.ServerPort, nameof(options.Value.ServerPort));
            ArgumentException.ThrowIfNullOrEmpty(options.Value.ServerSslMode, nameof(options.Value.ServerSslMode));
            ArgumentException.ThrowIfNullOrEmpty(options.Value.DatabaseName, nameof(options.Value.DatabaseName));
            ArgumentException.ThrowIfNullOrEmpty(options.Value.DatabaseUserLogin, nameof(options.Value.DatabaseUserLogin));

            if (!options.Value.UseManagedIdentity)
            {
                ArgumentException.ThrowIfNullOrEmpty(options.Value.DatabaseUserPassword, nameof(options.Value.DatabaseUserPassword));
            }

            if (options.Value.ServerName.Contains(options.Value.ServerNameSuffix, StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException($"'{nameof(options.Value.ServerName)}' should only contain the name of the server like 'myserver' and NOT the full host name like 'myserver{options.Value.ServerNameSuffix}'", nameof(options.Value.ServerName));
            }

            var connectionStringBuilder = new StringBuilder();
            connectionStringBuilder.Append($"Server={options.Value.ServerName}{options.Value.ServerNameSuffix};");
            connectionStringBuilder.Append($"Database={options.Value.DatabaseName};");
            connectionStringBuilder.Append($"Port={options.Value.ServerPort};");
            connectionStringBuilder.Append($"User Id={options.Value.DatabaseUserLogin};");

            if (!options.Value.UseManagedIdentity)
            {
                connectionStringBuilder.Append($"Password={options.Value.DatabaseUserPassword};");
            }

            connectionStringBuilder.Append($"SSL Mode={options.Value.ServerSslMode};");
            return connectionStringBuilder.ToString();
        }
    }
}
