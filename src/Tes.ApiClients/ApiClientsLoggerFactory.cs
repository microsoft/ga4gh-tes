// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Logging;

namespace Tes.ApiClients
{
    public static class ApiClientsLoggerFactory
    {
        private const string LogLevelEnvVariableName = "API_LOG_LEVEL";
        private static readonly ILoggerFactory SLogFactory = LoggerFactory.Create(builder =>
        {
            builder
                .AddSystemdConsole(options =>
                {
                    options.IncludeScopes = true;
                    options.TimestampFormat = "yyyy-MM-dd HH:mm:ss.fff ";
                    options.UseUtcTimestamp = true;
                });
            var logLevel = LogLevel.Information;

            if (Enum.TryParse<LogLevel>(Environment.GetEnvironmentVariable(LogLevelEnvVariableName), out var userLevel))
            {
                logLevel = userLevel;
            }

            builder.SetMinimumLevel(logLevel);
        });


        public static ILogger<T> Create<T>()
        {
            return SLogFactory.CreateLogger<T>();
        }
    }
}
