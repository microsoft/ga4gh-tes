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
            ArgumentException.ThrowIfNullOrEmpty(postgreSqlServerName, nameof(postgreSqlServerName));
            ArgumentException.ThrowIfNullOrEmpty(postgreSqlTesDatabaseName, nameof(postgreSqlTesDatabaseName));
            ArgumentException.ThrowIfNullOrEmpty(postgreSqlTesDatabasePort, nameof(postgreSqlTesDatabasePort));
            ArgumentException.ThrowIfNullOrEmpty(postgreSqlTesUserLogin, nameof(postgreSqlTesUserLogin));
            ArgumentException.ThrowIfNullOrEmpty(postgreSqlTesUserPassword, nameof(postgreSqlTesUserPassword));

            if (postgreSqlServerName.Contains(postgreSqlServerNameSuffix, StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException($"'{nameof(postgreSqlServerName)}' should only contain the name of the server like 'myserver' and NOT the full host name like 'myserver{postgreSqlServerNameSuffix}'", nameof(postgreSqlServerName));
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
