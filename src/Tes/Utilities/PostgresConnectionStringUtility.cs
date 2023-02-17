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
            ArgumentException.ThrowIfNullOrEmpty(options.Value.PostgreSqlServerName, nameof(options.Value.PostgreSqlServerName));
            ArgumentException.ThrowIfNullOrEmpty(options.Value.PostgreSqlServerNameSuffix, nameof(options.Value.PostgreSqlServerNameSuffix));
            ArgumentException.ThrowIfNullOrEmpty(options.Value.PostgreSqlServerPort, nameof(options.Value.PostgreSqlServerPort));
            ArgumentException.ThrowIfNullOrEmpty(options.Value.PostgreSqlServerSslMode, nameof(options.Value.PostgreSqlServerSslMode));
            ArgumentException.ThrowIfNullOrEmpty(options.Value.PostgreSqlDatabaseName, nameof(options.Value.PostgreSqlDatabaseName));
            ArgumentException.ThrowIfNullOrEmpty(options.Value.PostgreSqlDatabaseUserLogin, nameof(options.Value.PostgreSqlDatabaseUserLogin));
            ArgumentException.ThrowIfNullOrEmpty(options.Value.PostgreSqlDatabaseUserPassword, nameof(options.Value.PostgreSqlDatabaseUserPassword));
            

            if (options.Value.PostgreSqlServerName.Contains(options.Value.PostgreSqlServerNameSuffix, StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException($"'{nameof(options.Value.PostgreSqlServerName)}' should only contain the name of the server like 'myserver' and NOT the full host name like 'myserver{options.Value.PostgreSqlServerNameSuffix}'", nameof(options.Value.PostgreSqlServerName));
            }

            var connectionStringBuilder = new StringBuilder();
            connectionStringBuilder.Append($"Server={options.Value.PostgreSqlServerName}{options.Value.PostgreSqlServerNameSuffix};");
            connectionStringBuilder.Append($"Database={options.Value.PostgreSqlDatabaseName};");
            connectionStringBuilder.Append($"Port={options.Value.PostgreSqlServerPort};");
            connectionStringBuilder.Append($"User Id={options.Value.PostgreSqlDatabaseUserLogin};");
            connectionStringBuilder.Append($"Password={options.Value.PostgreSqlDatabaseUserPassword};");
            connectionStringBuilder.Append($"SSL Mode={options.Value.PostgreSqlServerSslMode};");
            return connectionStringBuilder.ToString();
        }
    }
}
