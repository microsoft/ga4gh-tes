using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
            connectionStringBuilder.Append("SSL Mode=Require;");
            return connectionStringBuilder.ToString();
        }
    }
}
