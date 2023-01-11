using System;
using System.Text;

namespace Tes.Utilities
{
    public class ConnectionStringUtility
    {
        public string GetPostgresConnectionString(
            string postgreSqlServerName, 
            string postgreSqlTesDatabaseName, 
            string postgreSqlTesDatabasePort, 
            string postgreSqlTesUserLogin, 
            string postgreSqlTesUserPassword,
            string postgreSqlServerNameSuffix = ".postgres.database.azure.com",
            string sslMode = "VerifyFull")
        {
            if (string.IsNullOrEmpty(postgreSqlServerName))
            {
                throw new ArgumentException($"'{nameof(postgreSqlServerName)}' cannot be null or empty.", nameof(postgreSqlServerName));
            }

            if (postgreSqlServerName.Contains(postgreSqlServerNameSuffix, StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException($"'{nameof(postgreSqlServerName)}' should only contain the name of the server like 'myserver' and NOT the full host name like 'myserver{postgreSqlServerNameSuffix}'", nameof(postgreSqlServerName));
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
            connectionStringBuilder.Append($"Server={postgreSqlServerName}{postgreSqlServerNameSuffix};");
            connectionStringBuilder.Append($"Database={postgreSqlTesDatabaseName};");
            connectionStringBuilder.Append($"Port={postgreSqlTesDatabasePort};");
            connectionStringBuilder.Append($"User Id={postgreSqlTesUserLogin};");
            connectionStringBuilder.Append($"Password={postgreSqlTesUserPassword};");
            connectionStringBuilder.Append($"SSL Mode={sslMode};");
            return connectionStringBuilder.ToString();
        }
    }
}
