// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Repository.Models
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

        public string ServerName { get; set; }
        public string ServerNameSuffix { get; set; } = ".postgres.database.azure.com";
        public string ServerPort { get; set; } = "5432";
        public string ServerSslMode { get; set; } = "VerifyFull";
        public string DatabaseName { get; set; } = "tes_db";
        public string DatabaseUserLogin { get; set; }
        public string DatabaseUserPassword { get; set; }
    }
}
