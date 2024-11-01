// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Azure.ResourceManager;
using CommonUtilities;
using CommonUtilities.AzureCloud;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.ApplicationInsights;
using TesApi.Web.Management;
using TesApi.Web.Options;
using TesApi.Web.Runner;

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
        public static async Task Main(string[] args)
        {
            try
            {
                await CreateWebHostBuilder(args).Build().RunAsync();
            }
            catch (Exception exc)
            {
                Console.WriteLine($"Main critical error: {exc.Message} {exc}");
                throw;
            }
        }

        /// <summary>
        /// Create the web host builder.
        /// </summary>
        /// <param name="args"></param>
        /// <returns><see cref="IWebHostBuilder"/></returns>
        public static IWebHostBuilder CreateWebHostBuilder(string[] args)
        {
            Console.WriteLine($"TES v{Startup.TesVersion} Build: {Assembly.GetExecutingAssembly().GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion}");

            Options.ApplicationInsightsOptions applicationInsightsOptions = default;
            var builder = WebHost.CreateDefaultBuilder<Startup>(args);

            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("ASPNETCORE_URLS")))
            {
                builder.UseUrls("http://0.0.0.0:80");
            }

            builder.ConfigureAppConfiguration((context, configBuilder) =>
            {
                configBuilder.AddEnvironmentVariables();
                var config = configBuilder.Build();
                Startup.AzureCloudConfig = GetAzureCloudConfig(config);
                StorageUrlUtils.BlobEndpointHostNameSuffix = $".blob.{Startup.AzureCloudConfig.Suffixes.StorageSuffix}";
                applicationInsightsOptions = GetApplicationInsightsConnectionString(
                    config,
                    Startup.AzureCloudConfig.ArmEnvironment.Value,
                    string.IsNullOrWhiteSpace(config["AzureServicesAuthConnectionString"]) ? null : new AzureServicesConnectionStringCredential(new(config, Startup.AzureCloudConfig)));

                if (!string.IsNullOrEmpty(applicationInsightsOptions?.ConnectionString))
                {
                    configBuilder.AddApplicationInsightsSettings(applicationInsightsOptions.ConnectionString, developerMode: context.HostingEnvironment.IsDevelopment() ? true : null);
                }

                static ApplicationInsightsOptions GetApplicationInsightsConnectionString(IConfiguration configuration, ArmEnvironment armEnvironment, Azure.Core.TokenCredential credential)
                {
                    var applicationInsightsOptions = configuration.GetSection(Options.ApplicationInsightsOptions.SectionName).Get<Options.ApplicationInsightsOptions>();
                    var applicationInsightsAccountName = applicationInsightsOptions?.AccountName;

                    if (applicationInsightsAccountName is null || !string.IsNullOrWhiteSpace(applicationInsightsOptions?.ConnectionString))
                    {
                        return applicationInsightsOptions;
                    }

                    var applicationInsightsConnectionString = configuration["APPLICATIONINSIGHTS_CONNECTION_STRING"];

                    if (string.IsNullOrWhiteSpace(applicationInsightsConnectionString) && credential is not null)
                    {
                        applicationInsightsConnectionString = ArmResourceInformationFinder.GetAppInsightsConnectionStringFromAccountNameAsync(applicationInsightsAccountName, credential, armEnvironment, CancellationToken.None).Result;
                    }

                    if (!string.IsNullOrWhiteSpace(applicationInsightsConnectionString))
                    {
                        applicationInsightsOptions ??= new Options.ApplicationInsightsOptions();
                        applicationInsightsOptions.ConnectionString = applicationInsightsConnectionString;
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
                        // This configures Container logging in AKS
                        logging.AddSimpleConsole(options =>
                        {
                            options.IncludeScopes = true;
                            options.SingleLine = true;
                            options.UseUtcTimestamp = true;
                        });
                        logging.AddConsole(options => options.LogToStandardErrorThreshold = LogLevel.Warning);

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

                    // Optional: Apply filters to configure LogLevel
                    // Trace or above is sent to ApplicationInsights for all categories.

                    // Additional filtering For category starting in "System",
                    // only Warning or above will be sent to Application Insights.
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

            static AzureCloudConfig GetAzureCloudConfig(IConfiguration configuration)
            {
                var tesOptions = new GeneralOptions();
                configuration.Bind(GeneralOptions.SectionName, tesOptions);
                Console.WriteLine($"tesOptions.AzureCloudName: {tesOptions.AzureCloudName}");
                return AzureCloudConfig.FromKnownCloudNameAsync(cloudName: tesOptions.AzureCloudName, azureCloudMetadataUrlApiVersion: tesOptions.AzureCloudMetadataUrlApiVersion).Result;
            }
        }
    }
}
