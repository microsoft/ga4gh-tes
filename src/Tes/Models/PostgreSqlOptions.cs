namespace Tes.Models
{
    /// <summary>
    /// PostgresSql configuration options
    /// </summary>
    public class PostgreSqlOptions
    {
        public const string PostgreSqlAccount = "PostgreSql";

        public string PostgreSqlServerName { get; set; }
        public string PostgreSqlServerNameSuffix { get; set; } = ".postgres.database.azure.com";
        public string PostgreSqlTesDatabaseName { get; set; } = "tes_db";
        public string PostgreSqlTesDatabasePort { get; set; } = "5432";
        public string PostgreSqlTesUserLogin { get; set; }
        public string PostgreSqlTesUserPassword { get; set; }
        public string PostgreSqlTesSslMode { get; set; } = "VerifyFull";
    }
}
