﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading;
using Azure.ResourceManager;
using CommonUtilities;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.ApplicationInsights;
using TesApi.Web.Management;

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
            ArmEnvironmentEndpoints armEnvironmentEndpoints = default;
            var builder = WebHost.CreateDefaultBuilder<Startup>(args);

            builder.ConfigureServices(services =>
            {
                services.AddSingleton(armEnvironmentEndpoints);
                services.AddTransient<AzureServicesConnectionStringCredentialOptions>();
            });

            builder.ConfigureAppConfiguration((context, config) =>
            {
                config.AddEnvironmentVariables(); // For Docker-Compose

                var configuration = config.Build();

                armEnvironmentEndpoints = GetArmEnvironment(configuration);

                applicationInsightsOptions = GetApplicationInsightsConnectionString(
                    configuration,
                    new(armEnvironmentEndpoints.ResourceManager, armEnvironmentEndpoints.Audience),
                    new AzureServicesConnectionStringCredential(new(configuration, armEnvironmentEndpoints)));

                if (!string.IsNullOrEmpty(applicationInsightsOptions?.ConnectionString))
                {
                    config.AddApplicationInsightsSettings(applicationInsightsOptions.ConnectionString, developerMode: context.HostingEnvironment.IsDevelopment() ? true : null);
                }

                static Options.ApplicationInsightsOptions GetApplicationInsightsConnectionString(IConfiguration configuration, ArmEnvironment armEnvironment, Azure.Core.TokenCredential credential)
                {
                    var applicationInsightsOptions = configuration.GetSection(Options.ApplicationInsightsOptions.SectionName).Get<Options.ApplicationInsightsOptions>();
                    var applicationInsightsAccountName = applicationInsightsOptions?.AccountName;

                    if (applicationInsightsAccountName is null || !string.IsNullOrWhiteSpace(applicationInsightsOptions?.ConnectionString))
                    {
                        return applicationInsightsOptions;
                    }

                    var applicationInsightsConnectionString = configuration["APPLICATIONINSIGHTS_CONNECTION_STRING"];

                    if (string.IsNullOrWhiteSpace(applicationInsightsConnectionString))
                    {
                        applicationInsightsConnectionString = ArmResourceInformationFinder.GetAppInsightsConnectionStringAsync(credential, armEnvironment, applicationInsightsAccountName, CancellationToken.None).Result;
                    }

                    if (!string.IsNullOrWhiteSpace(applicationInsightsConnectionString))
                    {
                        applicationInsightsOptions ??= new Options.ApplicationInsightsOptions();
                        applicationInsightsOptions.ConnectionString = applicationInsightsConnectionString;
                    }

                    return applicationInsightsOptions;
                }

                static ArmEnvironmentEndpoints GetArmEnvironment(IConfiguration configuration)
                {
                    var armEnvironmentOptions = configuration.GetSection(Options.ArmEnvironmentOptions.SectionName).Get<Options.ArmEnvironmentOptions>();
                    var armEndpoint = NullIfEnpty(armEnvironmentOptions?.Endpoint);
                    var armName = NullIfEnpty(armEnvironmentOptions?.Name);

                    if (armEndpoint is null && armName is null)
                    {
                        // Ask the VM for Name
                    }

                    try
                    {
                        return armEndpoint is null
                            ? ArmEnvironmentEndpoints.FromKnownCloudNameAsync(armName).GetAwaiter().GetResult()
                            : ArmEnvironmentEndpoints.FromMetadataEndpointsAsync(new(armEndpoint)).GetAwaiter().GetResult();
                    }
                    catch (ArgumentException exception)
                    {
                        throw new InvalidOperationException($"The azure cloud '{armName}' is not recognized. Please confgure {Options.ArmEnvironmentOptions.SectionName}:{nameof(Options.ArmEnvironmentOptions.Endpoint)}.", exception);
                    }

                    static string NullIfEnpty(string value)
                        => string.IsNullOrWhiteSpace(value) ? null : value;
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
