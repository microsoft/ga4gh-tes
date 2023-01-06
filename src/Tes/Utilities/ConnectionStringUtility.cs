using System;
using System.Text;

namespace Tes.Utilities
{
    public class ConnectionStringUtility
    {
        public string GetPostgresConnectionString(string postgreSqlServerName, string postgreSqlTesDatabaseName, string postgreSqlTesDatabasePort, string postgreSqlTesUserLogin, string postgreSqlTesUserPassword)
        {
            if (string.IsNullOrEmpty(postgreSqlServerName))
            {
                throw new ArgumentException($"'{nameof(postgreSqlServerName)}' cannot be null or empty.", nameof(postgreSqlServerName));
            }

            if (postgreSqlServerName.Contains(".postgres.database.azure.com", StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException($"'{nameof(postgreSqlServerName)}' should only contain the name of the server like 'myserver' and NOT the full host name like 'myserver.postgres.database.azure.com'", nameof(postgreSqlServerName));
            }

            if (string.IsNullOrEmpty(postgreSqlTesDatabaseName))
            {
                throw new ArgumentException($"'{nameof(postgreSqlTesDatabaseName)}' cannot be null or empty.", nameof(postgreSqlTesDatabaseName));
            }

            if (string.IsNullOrEmpty(postgreSqlTesDatabasePort))
            {
                throw new ArgumentException($"'{nameof(postgreSqlTesDatabasePort)}' cannot be null or empty.", nameof(postgreSqlTesDatabasePort));
            }

            if (string.IsNullOrEmpty(postgreSqlTesUserLogin))
            {
                throw new ArgumentException($"'{nameof(postgreSqlTesUserLogin)}' cannot be null or empty.", nameof(postgreSqlTesUserLogin));
            }

            if (string.IsNullOrEmpty(postgreSqlTesUserPassword))
            {
                throw new ArgumentException($"'{nameof(postgreSqlTesUserPassword)}' cannot be null or empty.", nameof(postgreSqlTesUserPassword));
            }

            var connectionStringBuilder = new StringBuilder();
            connectionStringBuilder.Append($"Server={postgreSqlServerName}.postgres.database.azure.com;");
            connectionStringBuilder.Append($"Database={postgreSqlTesDatabaseName};");
            connectionStringBuilder.Append($"Port={postgreSqlTesDatabasePort};");
            connectionStringBuilder.Append($"User Id={postgreSqlTesUserLogin};");
            connectionStringBuilder.Append($"Password={postgreSqlTesUserPassword};");
            connectionStringBuilder.Append("SSL Mode=Prefer;"); // TODO, setting "SSL Mode=Require" results in: One or more errors occurred. (To validate server certificates, please use VerifyFull or VerifyCA instead of Require. To disable validation, explicitly set 'Trust Server Certificate' to true. See https://www.npgsql.org/doc/release-notes/6.0.html for more details.
            return connectionStringBuilder.ToString();
        }
    }
}
