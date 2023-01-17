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
            ArgumentException.ThrowIfNullOrEmpty(options.Value.PostgreSqlTesDatabaseName, nameof(options.Value.PostgreSqlTesDatabaseName));
            ArgumentException.ThrowIfNullOrEmpty(options.Value.PostgreSqlTesDatabasePort, nameof(options.Value.PostgreSqlTesDatabasePort));
            ArgumentException.ThrowIfNullOrEmpty(options.Value.PostgreSqlTesUserLogin, nameof(options.Value.PostgreSqlTesUserLogin));
            ArgumentException.ThrowIfNullOrEmpty(options.Value.PostgreSqlTesUserPassword, nameof(options.Value.PostgreSqlTesUserPassword));

            if (options.Value.PostgreSqlServerName.Contains(options.Value.PostgreSqlServerNameSuffix, StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException($"'{nameof(options.Value.PostgreSqlServerName)}' should only contain the name of the server like 'myserver' and NOT the full host name like 'myserver{options.Value.PostgreSqlServerNameSuffix}'", nameof(options.Value.PostgreSqlServerName));
            }

            var connectionStringBuilder = new StringBuilder();
            connectionStringBuilder.Append($"Server={options.Value.PostgreSqlServerName}{options.Value.PostgreSqlServerNameSuffix};");
            connectionStringBuilder.Append($"Database={options.Value.PostgreSqlTesDatabaseName};");
            connectionStringBuilder.Append($"Port={options.Value.PostgreSqlTesDatabasePort};");
            connectionStringBuilder.Append($"User Id={options.Value.PostgreSqlTesUserLogin};");
            connectionStringBuilder.Append($"Password={options.Value.PostgreSqlTesUserPassword};");
            connectionStringBuilder.Append($"SSL Mode={options.Value.PostgreSqlSslMode};");
            return connectionStringBuilder.ToString();
        }
    }
}
