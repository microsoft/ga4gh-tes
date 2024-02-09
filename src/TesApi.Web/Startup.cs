﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.IO;
using System.Reflection;
using Azure.Core;
using Azure.Identity;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;
using Tes.ApiClients;
using Tes.ApiClients.Options;
using Tes.Models;
using Tes.Repository;
using TesApi.Filters;
using TesApi.Web.Management;
using TesApi.Web.Management.Batch;
using TesApi.Web.Management.Configuration;
using TesApi.Web.Options;
using TesApi.Web.Runner;
using TesApi.Web.Storage;

namespace TesApi.Web
{
    /// <summary>
    /// Startup
    /// </summary>
    public class Startup
    {
        // TODO centralize in single location
        private const string TesVersion = "5.2.0";
        private readonly IConfiguration configuration;
        private readonly ILogger logger;
        private readonly IWebHostEnvironment hostingEnvironment;

        /// <summary>
        /// Startup class for ASP.NET core
        /// </summary>
        public Startup(IConfiguration configuration, ILogger<Startup> logger, IWebHostEnvironment hostingEnvironment)
        {
            this.configuration = configuration;
            this.hostingEnvironment = hostingEnvironment;
            this.logger = logger;
        }

        /// <summary>
        /// This method gets called by the runtime. Use this method to add services to the container.
        /// </summary>
        /// <param name="services">The Microsoft.Extensions.DependencyInjection.IServiceCollection to add the services to.</param>
        public void ConfigureServices(IServiceCollection services)
        {
            try
            {
                services
                    .AddLogging()
                    .AddApplicationInsightsTelemetry(configuration)

                    .Configure<BatchAccountOptions>(configuration.GetSection(BatchAccountOptions.SectionName))
                    .Configure<PostgreSqlOptions>(configuration.GetSection(PostgreSqlOptions.GetConfigurationSectionName("Tes")))
                    .Configure<RetryPolicyOptions>(configuration.GetSection(RetryPolicyOptions.SectionName))
                    .Configure<TerraOptions>(configuration.GetSection(TerraOptions.SectionName))
                    .Configure<BatchImageGeneration1Options>(configuration.GetSection(BatchImageGeneration1Options.SectionName))
                    .Configure<BatchImageGeneration2Options>(configuration.GetSection(BatchImageGeneration2Options.SectionName))
                    .Configure<BatchNodesOptions>(configuration.GetSection(BatchNodesOptions.SectionName))
                    .Configure<BatchSchedulingOptions>(configuration.GetSection(BatchSchedulingOptions.SectionName))
                    .Configure<StorageOptions>(configuration.GetSection(StorageOptions.SectionName))
                    .Configure<MarthaOptions>(configuration.GetSection(MarthaOptions.SectionName))

                    .AddMemoryCache(o => o.ExpirationScanFrequency = TimeSpan.FromHours(12))
                    .AddSingleton<ICache<TesTaskDatabaseItem>, TesRepositoryCache<TesTaskDatabaseItem>>()
                    .AddSingleton<TesTaskPostgreSqlRepository>()
                    .AddSingleton<AzureProxy>()
                    .AddTransient<BatchPool>()
                    .AddSingleton<IBatchPoolFactory, BatchPoolFactory>()
                    .AddSingleton(CreateTerraApiClient)
                    .AddSingleton(CreateBatchPoolManagerFromConfiguration)

                    .AddControllers(options => options.Filters.Add<Controllers.OperationCancelledExceptionFilter>())
                        .AddNewtonsoftJson(opts =>
                        {
                            opts.SerializerSettings.ContractResolver = new CamelCasePropertyNamesContractResolver();
                            opts.SerializerSettings.Converters.Add(new StringEnumConverter(new CamelCaseNamingStrategy()));
                        })
                    .Services

                    .AddSingleton(CreateStorageAccessProviderFromConfiguration)
                    .AddSingleton<IAzureProxy>(sp => ActivatorUtilities.CreateInstance<CachingWithRetriesAzureProxy>(sp, (IAzureProxy)sp.GetRequiredService(typeof(AzureProxy))))
                    .AddSingleton<IRepository<TesTask>>(sp => ActivatorUtilities.CreateInstance<RepositoryRetryHandler<TesTask>>(sp, (IRepository<TesTask>)sp.GetRequiredService(typeof(TesTaskPostgreSqlRepository))))

                    .AddAutoMapper(typeof(MappingProfilePoolToWsmRequest))
                    .AddSingleton<CachingRetryHandler>()
                    .AddSingleton<IBatchQuotaVerifier, BatchQuotaVerifier>()
                    .AddSingleton<IBatchScheduler, BatchScheduler>()
                    .AddSingleton<PriceApiClient>()
                    .AddSingleton<IBatchSkuInformationProvider, PriceApiBatchSkuInformationProvider>()
                    .AddSingleton(CreateBatchAccountResourceInformation)
                    .AddSingleton(CreateBatchQuotaProviderFromConfiguration)
                    .AddSingleton<AzureManagementClientsFactory>()
                    .AddSingleton<ConfigurationUtils>()
                    .AddSingleton<IAllowedVmSizesService, AllowedVmSizesService>()
                    .AddSingleton<TokenCredential>(s => new DefaultAzureCredential())
                    .AddSingleton<TaskToNodeTaskConverter>()
                    .AddSingleton<TaskExecutionScriptingManager>()
                    .AddTransient<BatchNodeScriptBuilder>()

                    .AddSwaggerGen(c =>
                    {
                        c.SwaggerDoc(TesVersion, new()
                        {
                            Version = TesVersion,
                            Title = "GA4GH Task Execution Service",
                            Description = "Task Execution Service (ASP.NET Core 7.0)",
                            Contact = new()
                            {
                                Name = "Microsoft Biomedical Platforms and Genomics",
                                Url = new("https://github.com/ga4gh/tes")
                            },
                            License = new()
                            {
                                Name = "MIT License",
                                // Identifier = "MIT" //TODO: when available, remove Url -- https://spec.openapis.org/oas/v3.1.0#fixed-fields-2
                                Url = new("https://spdx.org/licenses/MIT", UriKind.Absolute)
                            },
                        });
                        c.CustomSchemaIds(type => type.FullName);
                        c.IncludeXmlComments(
                            $"{AppContext.BaseDirectory}{Path.DirectorySeparatorChar}{Assembly.GetEntryAssembly().GetName().Name}.xml");
                        c.OperationFilter<GeneratePathParamsValidationFilter>();
                    })

                    // Order is important for hosted services
                    .AddHostedService(sp => (AllowedVmSizesService)sp.GetRequiredService(typeof(IAllowedVmSizesService)))
                    .AddHostedService<BatchPoolService>()
                    .AddHostedService<Scheduler>();
            }
            catch (Exception exc)
            {
                logger?.LogCritical(exc, @"TES could not start: {ExceptionMessage}", exc.Message);
                Console.WriteLine($"TES could not start: {exc}");
                throw;
            }

            logger?.LogInformation("TES successfully configured dependent services in ConfigureServices(IServiceCollection services)");


            IBatchQuotaProvider CreateBatchQuotaProviderFromConfiguration(IServiceProvider services)
            {
                logger.LogInformation("Attempting to create a Batch Quota Provider");

                if (TerraOptionsAreConfigured(services))
                {
                    logger.LogInformation("Using the Terra Quota Provider.");

                    return ActivatorUtilities.CreateInstance<TerraQuotaProvider>(services);
                }

                logger.LogInformation("Using default ARM Quota Provider.");

                return ActivatorUtilities.CreateInstance<ArmBatchQuotaProvider>(services);
            }

            IBatchPoolManager CreateBatchPoolManagerFromConfiguration(IServiceProvider services)
            {
                logger.LogInformation("Attempting to create a Batch Pool Manager");

                if (TerraOptionsAreConfigured(services))
                {
                    logger.LogInformation("Using Terra Batch Pool Manager");

                    return ActivatorUtilities.CreateInstance<TerraBatchPoolManager>(services);
                }

                logger.LogInformation("Using default Batch Pool Manager.");

                return ActivatorUtilities.CreateInstance<ArmBatchPoolManager>(services);
            }

            IStorageAccessProvider CreateStorageAccessProviderFromConfiguration(IServiceProvider services)
            {
                logger.LogInformation("Attempting to create a Storage Access Provider");

                if (TerraOptionsAreConfigured(services))
                {
                    logger.LogInformation("Using Terra Storage Provider");

                    return ActivatorUtilities.CreateInstance<TerraStorageAccessProvider>(services);
                }

                logger.LogInformation("Using Default Storage Provider");

                return ActivatorUtilities.CreateInstance<DefaultStorageAccessProvider>(services);
            }

            bool TerraOptionsAreConfigured(IServiceProvider services)
            {
                var options = services.GetRequiredService<IOptions<TerraOptions>>();

                //if workspace id is set, then we are assuming we are running in terra
                if (!string.IsNullOrEmpty(options.Value.WorkspaceId))
                {
                    ValidateRequiredOptionsForTerraStorageProvider(options.Value);

                    return true;
                }

                return false;
            }

            TerraWsmApiClient CreateTerraApiClient(IServiceProvider services)
            {
                logger.LogInformation("Attempting to create a Terra WSM API client");

                if (TerraOptionsAreConfigured(services))
                {
                    var options = services.GetRequiredService<IOptions<TerraOptions>>();

                    ValidateRequiredOptionsForTerraStorageProvider(options.Value);

                    return ActivatorUtilities.CreateInstance<TerraWsmApiClient>(services, options.Value.WsmApiHost);
                }

                throw new InvalidOperationException("Terra WSM API Host is not configured.");
            }

            static void ValidateRequiredOptionsForTerraStorageProvider(TerraOptions terraOptions)
            {
                ArgumentException.ThrowIfNullOrEmpty(terraOptions.WorkspaceId, nameof(terraOptions.WorkspaceId));
                ArgumentException.ThrowIfNullOrEmpty(terraOptions.WorkspaceStorageAccountName, nameof(terraOptions.WorkspaceStorageAccountName));
                ArgumentException.ThrowIfNullOrEmpty(terraOptions.WorkspaceStorageContainerName, nameof(terraOptions.WorkspaceStorageContainerName));
                ArgumentException.ThrowIfNullOrEmpty(terraOptions.WorkspaceStorageContainerResourceId, nameof(terraOptions.WorkspaceStorageContainerResourceId));
                ArgumentException.ThrowIfNullOrEmpty(terraOptions.WsmApiHost, nameof(terraOptions.WsmApiHost));
            }

            BatchAccountResourceInformation CreateBatchAccountResourceInformation(IServiceProvider services)
            {
                var options = services.GetRequiredService<IOptions<BatchAccountOptions>>();

                if (string.IsNullOrEmpty(options.Value.AccountName))
                {
                    throw new InvalidOperationException(
                        "The batch account name is missing. Please check your configuration.");
                }

                if (string.IsNullOrWhiteSpace(options.Value.AppKey))
                {
                    //we are assuming Arm with MI/RBAC if no key is provided. Try to get info from the batch account.
                    var task = ArmResourceInformationFinder.TryGetResourceInformationFromAccountNameAsync(options.Value.AccountName, System.Threading.CancellationToken.None);
                    task.Wait();

                    if (task.Result is null)
                    {
                        throw new InvalidOperationException(
                            $"Failed to get the resource information for the Batch account using ARM. Please check the options provided. Provided Batch account name:{options.Value.AccountName}");
                    }

                    return task.Result;
                }

                //assume the information was provided via configuration
                return new BatchAccountResourceInformation(options.Value.AccountName, options.Value.ResourceGroup, options.Value.SubscriptionId, options.Value.Region, options.Value.BaseUrl);
            }
        }


        /// <summary>
        /// This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        /// </summary>
        /// <param name="app">An Microsoft.AspNetCore.Builder.IApplicationBuilder for the app to configure.</param>
        public void Configure(IApplicationBuilder app)
        {
            try
            {
                app.UseRouting()
                    .UseEndpoints(endpoints =>
                    {
                        endpoints.MapControllers();
                    })

                    .UseHttpsRedirection()

                    .UseDefaultFiles()
                    .UseStaticFiles()
                    .UseSwagger(c =>
                    {
                        c.RouteTemplate = "swagger/{documentName}/openapi.json";
                    })

                    .IfThenElse(hostingEnvironment.IsDevelopment(),
                        s =>
                        {
                            logger.LogInformation("Configuring for Development environment");
                            return s.UseDeveloperExceptionPage();
                        },
                        s =>
                        {
                            logger.LogInformation("Configuring for Production environment");
                            return s.UseHsts();
                        });
            }
            catch (Exception exc)
            {
                logger?.LogCritical(exc, @"TES could not start: {ExceptionMessage}", exc.Message);
                Console.WriteLine($"TES could not start: {exc}");
                throw;
            }
        }
    }

    internal static class BooleanMethodSelectorExtensions
    {
        public static IApplicationBuilder IfThenElse(this IApplicationBuilder builder, bool @if, Func<IApplicationBuilder, IApplicationBuilder> then, Func<IApplicationBuilder, IApplicationBuilder> @else)
            => @if ? then(builder) : @else(builder);

        public static IServiceCollection IfThenElse(this IServiceCollection services, bool @if, Func<IServiceCollection, IServiceCollection> then, Func<IServiceCollection, IServiceCollection> @else)
            => @if ? then(services) : @else(services);

        public static IServiceCollection IfThenElse(this IServiceCollection services, bool @if, Action<IServiceCollection> then, Action<IServiceCollection> @else)
            => services.IfThenElse(@if, s => services.Wrap(then), s => services.Wrap(@else));

        private static IServiceCollection Wrap(this IServiceCollection services, Action<IServiceCollection> action)
        {
            action?.Invoke(services);
            return services;
        }
    }
}
