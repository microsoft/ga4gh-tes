namespace Tes.Models
{
    /// <summary>
    /// PostgresSql configuration options
    /// </summary>
    public class PostgreSqlOptions
    {
        private const string ConfigurationSectionNameSuffix = "PostgreSql";

        /// <summary>
        /// Gets a configuration section name based on the service name.  Used since there are multiple Postgres databases in CoA
        /// </summary>
        /// <param name="serviceName">The name of the service, i.e. "Tes" or "Cromwell"</param>
        /// <returns>The configuration section name (usually in appsettings)</returns>
        public static string GetConfigurationSectionName(string serviceName = "Tes")
        {
            return $"{serviceName}{ConfigurationSectionNameSuffix}";
        }

        public string PostgreSqlServerName { get; set; }
        public string PostgreSqlServerNameSuffix { get; set; } = ".postgres.database.azure.com";
        public string PostgreSqlServerPort { get; set; } = "5432";
        public string PostgreSqlServerSslMode { get; set; } = "VerifyFull";
        public string PostgreSqlDatabaseName { get; set; } = "tes_db";
        public string PostgreSqlDatabaseUserLogin { get; set; }
        public string PostgreSqlDatabaseUserPassword { get; set; }
    }
}
