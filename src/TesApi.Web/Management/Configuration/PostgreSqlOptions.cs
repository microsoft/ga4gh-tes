namespace TesApi.Web.Management.Configuration
{
    /// <summary>
    /// PostgresSql configuration options
    /// </summary>
    public class PostgreSqlOptions
    {
        public const string PostgreSqlAccount = "PostgreSql";

        public string PostgreSqlServerName { get; set; }
        public string PostgreSqlTesDatabaseName { get; set; }
        public string PostgreSqlTesDatabasePort { get; set; }
        public string PostgreSqlTesUserLogin { get; set; }
        public string PostgreSqlTesUserPassword { get; set; }
    }
}
