// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.ApplicationInsights;

namespace TesApi.Web
{
    /// <summary>
    /// Program
    /// </summary>
    public class Program
    {
        /// <summary>
        /// Main
        /// </summary>
        /// <param name="args"></param>
        public static void Main(string[] args)
            => CreateWebHostBuilder(args).Build().Run();

        /// <summary>
        /// Create the web host builder.
        /// </summary>
        /// <param name="args"></param>
        /// <returns><see cref="IWebHostBuilder"/></returns>
        public static IWebHostBuilder CreateWebHostBuilder(string[] args)
        {
            string applicationInsightsConnectionString = default;
            var builder = WebHost.CreateDefaultBuilder<Startup>(args);

            builder.ConfigureAppConfiguration((context, config) =>
            {
                config.AddEnvironmentVariables(); // For Docker-Compose
                applicationInsightsConnectionString = GetApplicationInsightsConnectionString(config.Build());

                if (!string.IsNullOrEmpty(applicationInsightsConnectionString))
                {
                    config.AddApplicationInsightsSettings(applicationInsightsConnectionString, developerMode: context.HostingEnvironment.IsDevelopment() ? true : null);
                }

                static string GetApplicationInsightsConnectionString(IConfiguration configuration)
                {
                    var applicationInsightsAccountName = configuration.GetSection(Options.ApplicationInsightsOptions.SectionName).Get<Options.ApplicationInsightsOptions>()?.AccountName;
                    Console.WriteLine($"ApplicationInsightsAccountName: {applicationInsightsAccountName}");

                    if (applicationInsightsAccountName is null)
                    {
                        return default;
                    }

                    var instrumentationKey = AzureProxy.GetAppInsightsInstrumentationKeyAsync(applicationInsightsAccountName).Result;
                    return instrumentationKey is null ? string.Empty : $"InstrumentationKey={instrumentationKey}";
                }
            });

            builder.ConfigureLogging((context, logging) =>
            {
                try
                {
                    if (context.HostingEnvironment.IsProduction())
                    {
                        if (!string.IsNullOrEmpty(applicationInsightsConnectionString))
                        {
                            logging.AddApplicationInsights(configuration => configuration.ConnectionString = applicationInsightsConnectionString, options => { });
                        }
                    }
                    else
                    {
                        logging.AddApplicationInsights();
                        logging.AddDebug();
                        logging.AddConsole();
                    }

                    // Optional: Apply filters to configure LogLevel Trace or above is sent to
                    // ApplicationInsights for all categories.
                    logging.AddFilter<ApplicationInsightsLoggerProvider>("System", LogLevel.Warning);

                    // Additional filtering For category starting in "Microsoft",
                    // only Warning or above will be sent to Application Insights.
                    logging.AddFilter<ApplicationInsightsLoggerProvider>("Microsoft", LogLevel.Warning);

                    // The following configures LogLevel Information or above to be sent to
                    // Application Insights for categories starting with "TesApi".
                    logging.AddFilter<ApplicationInsightsLoggerProvider>("TesApi", LogLevel.Information);
                }
                catch (Exception exc)
                {
                    Console.WriteLine($"Exception while configuring logging: {exc}");
                    throw;
                }
            });

            return builder;
        }
    }
}
