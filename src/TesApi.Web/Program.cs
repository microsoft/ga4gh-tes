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
            Options.ApplicationInsightsOptions applicationInsightsOptions = default;
            var builder = WebHost.CreateDefaultBuilder<Startup>(args);

            builder.ConfigureAppConfiguration((context, config) =>
            {
                config.AddEnvironmentVariables(); // For Docker-Compose
                applicationInsightsOptions = GetApplicationInsightsConnectionString(config.Build());

                if (!string.IsNullOrEmpty(applicationInsightsOptions?.ConnectionString))
                {
                    config.AddApplicationInsightsSettings(applicationInsightsOptions.ConnectionString, developerMode: context.HostingEnvironment.IsDevelopment() ? true : null);
                }

                static Options.ApplicationInsightsOptions GetApplicationInsightsConnectionString(IConfiguration configuration)
                {
                    var applicationInsightsOptions = configuration.GetSection(Options.ApplicationInsightsOptions.SectionName).Get<Options.ApplicationInsightsOptions>();
                    var applicationInsightsAccountName = applicationInsightsOptions?.AccountName;
                    Console.WriteLine($"ApplicationInsightsAccountName: {applicationInsightsAccountName}");

                    if (applicationInsightsAccountName is null || !string.IsNullOrWhiteSpace(applicationInsightsOptions?.ConnectionString))
                    {
                        return applicationInsightsOptions;
                    }

                    var applicationInsightsConnectionString = configuration["APPLICATIONINSIGHTS_CONNECTION_STRING"];

                    if (!string.IsNullOrWhiteSpace(applicationInsightsConnectionString))
                    {
                        applicationInsightsOptions ??= new Options.ApplicationInsightsOptions();
                        applicationInsightsOptions.ConnectionString = applicationInsightsConnectionString;
                        return applicationInsightsOptions;
                    }

                    var connectionString = AzureProxy.GetAppInsightsConnectionStringAsync(applicationInsightsAccountName, System.Threading.CancellationToken.None).Result;
                    if (!string.IsNullOrWhiteSpace(connectionString))
                    {
                        applicationInsightsOptions.ConnectionString = connectionString;
                    }

                    return applicationInsightsOptions;
                }
            });

            builder.ConfigureLogging((context, logging) =>
            {
                try
                {
                    if (context.HostingEnvironment.IsProduction())
                    {

                        if (!string.IsNullOrEmpty(applicationInsightsOptions?.ConnectionString))
                        {
                            logging.AddApplicationInsights(configuration => configuration.ConnectionString = applicationInsightsOptions.ConnectionString, options => { });
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

                    // The following configures LogLevel Information or above to be sent to
                    // Application Insights for categories starting with "Tes".
                    logging.AddFilter<ApplicationInsightsLoggerProvider>("Tes", LogLevel.Information);
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
