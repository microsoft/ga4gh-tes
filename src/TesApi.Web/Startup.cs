﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.IO;
using System.Reflection;
using AutoMapper;
using Azure.Core;
using Azure.Identity;
using LazyCache;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.OpenApi.Models;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;
using Tes.Models;
using Tes.Repository;
using TesApi.Filters;
using TesApi.Web.Management;
using TesApi.Web.Management.Batch;
using TesApi.Web.Management.Clients;
using TesApi.Web.Management.Configuration;
using TesApi.Web.Storage;

namespace TesApi.Web
{
    /// <summary>
    /// Startup
    /// </summary>
    public class Startup
    {
        private const string CosmosDbDatabaseId = "TES";
        private const string CosmosDbContainerId = "Tasks";
        private const string CosmosDbPartitionId = "01";

        private readonly ILogger logger;
        private readonly IWebHostEnvironment hostingEnvironment;

        /// <summary>
        /// Startup class for ASP.NET core
        /// </summary>
        public Startup(IConfiguration configuration, ILogger<Startup> logger, IWebHostEnvironment hostingEnvironment)
        {
            Configuration = configuration;
            this.hostingEnvironment = hostingEnvironment;
            this.logger = logger;
        }

        /// <summary>
        /// The application configuration
        /// </summary>
        private IConfiguration Configuration { get; }

        /// <summary>
        /// This method gets called by the runtime. Use this method to add services to the container.
        /// </summary>
        /// <param name="services">The Microsoft.Extensions.DependencyInjection.IServiceCollection to add the services to.</param>
        public void ConfigureServices(IServiceCollection services)
        {
            services
                .Configure<BatchAccountOptions>(Configuration.GetSection(BatchAccountOptions.SectionName))
                .Configure<CosmosDbOptions>(Configuration.GetSection(CosmosDbOptions.CosmosDbAccount))
                .Configure<RetryPolicyOptions>(Configuration.GetSection(RetryPolicyOptions.SectionName))
                .Configure<TerraOptions>(Configuration.GetSection(TerraOptions.SectionName))
                .Configure<ContainerRegistryOptions>(Configuration.GetSection(ContainerRegistryOptions.SectionName))

                .AddLogging()

                .AddSingleton<IAppCache, CachingService>()
                .AddSingleton<AzureProxy>()
                .AddSingleton(CreateCosmosDbRepositoryFromConfiguration)
                .AddSingleton<IBatchPoolFactory, BatchPoolFactory>()
                .AddTransient<BatchPool>()
                .AddSingleton(CreateBatchPoolManagerFromConfiguration)

                .AddControllers()
                .AddNewtonsoftJson(opts =>
                {
                    opts.SerializerSettings.ContractResolver = new CamelCasePropertyNamesContractResolver();
                    opts.SerializerSettings.Converters.Add(new StringEnumConverter(new CamelCaseNamingStrategy()));
                }).Services

                .AddSingleton<IBatchScheduler, BatchScheduler>()
                .AddSingleton(CreateStorageAccessProviderFromConfiguration)
                .AddSingleton<IAzureProxy>(sp => ActivatorUtilities.CreateInstance<CachingWithRetriesAzureProxy>(sp, (IAzureProxy)sp.GetRequiredService(typeof(AzureProxy))))


                .AddAutoMapper(typeof(MappingProfilePoolToWsmRequest))
                .AddSingleton<ContainerRegistryProvider>()
                .AddSingleton<CacheAndRetryHandler>()
                .AddSingleton<IBatchQuotaVerifier, BatchQuotaVerifier>()
                .AddSingleton<IBatchScheduler, BatchScheduler>()
                .AddSingleton<PriceApiClient>()
                .AddSingleton<IBatchSkuInformationProvider, PriceApiBatchSkuInformationProvider>()
                .AddSingleton(CreateBatchAccountResourceInformation)
                .AddSingleton(CreateBatchQuotaProviderFromConfiguration)
                .AddSingleton<AzureManagementClientsFactory>()
                .AddSingleton<ConfigurationUtils>()
                .AddSingleton<TokenCredential>(s => new DefaultAzureCredential())

                .AddSwaggerGen(c =>
                {
                    c.SwaggerDoc("0.4.0", new OpenApiInfo
                    {
                        Version = "0.4.0",
                        Title = "Task Execution Service",
                        Description = "Task Execution Service (ASP.NET Core 7.0)",
                        Contact = new OpenApiContact()
                        {
                            Name = "Microsoft Biomedical Platforms and Genomics",
                            Url = new Uri("https://github.com/microsoft/CromwellOnAzure")
                        },
                        License = new OpenApiLicense()
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

                .AddHostedService<DoOnceAtStartUpService>()
                .AddHostedService<BatchPoolService>()
                .AddHostedService<Scheduler>()
                .AddHostedService<DeleteCompletedBatchJobsHostedService>()
                .AddHostedService<DeleteOrphanedBatchJobsHostedService>()
                .AddHostedService<DeleteOrphanedAutoPoolsHostedService>();
            //.AddHostedService<RefreshVMSizesAndPricesHostedService>()


            AddAppInsightsWithDefaultOrLookingUpTheConnectionString(services);
        }

        private void AddAppInsightsWithDefaultOrLookingUpTheConnectionString(IServiceCollection services)
        {
            var accountName = Configuration["ApplicationInsightsAccountName"];

            if (string.IsNullOrEmpty(accountName))
            {
                //use default settings that will use the app insights configuration
                services.AddApplicationInsightsTelemetry();
                return;
            }

            services.AddApplicationInsightsTelemetry(s =>
            {
                var instrumentationKey = ArmResourceInformationFinder
                    .GetAppInsightsInstrumentationKeyAsync(accountName)
                    .Result;

                s.ConnectionString = $"InstrumentationKey={instrumentationKey}";
            });
        }
        private IBatchQuotaProvider CreateBatchQuotaProviderFromConfiguration(IServiceProvider services)
        {
            var terraOptions = services.GetService<IOptions<TerraOptions>>();

            logger.LogInformation("Attempting to create a Batch Quota Provider");

            logger.LogInformation("Attempting to create a Batch Quota Provider");

            if (!string.IsNullOrEmpty(terraOptions?.Value.LandingZoneApiHost))
            {
                var terraApiClient = ActivatorUtilities.CreateInstance<TerraLandingZoneApiClient>(services);

                logger.LogInformation("Terra Landing Zone API Host is set. Using the Terra Quota Provider.");

                return new TerraQuotaProvider(terraApiClient, terraOptions);
            }

            logger.LogInformation("Using default ARM Quota Provider.");

            return ActivatorUtilities.CreateInstance<ArmBatchQuotaProvider>(services);
        }

        private IBatchPoolManager CreateBatchPoolManagerFromConfiguration(IServiceProvider services)
        {
            var terraOptions = services.GetService<IOptions<TerraOptions>>();

            logger.LogInformation("Attempting to create a Batch Pool Manager");

            logger.LogInformation("Attempting to create a Batch Pool Manager");

            if (!string.IsNullOrEmpty(terraOptions?.Value.WsmApiHost))
            {
                logger.LogInformation("Terra WSM API Host is set. Using Terra Batch Pool Manager");

                return new TerraBatchPoolManager(
                    ActivatorUtilities.CreateInstance<TerraWsmApiClient>(services),
                    services.GetRequiredService<IMapper>(),
                    terraOptions,
                    services.GetService<IOptions<BatchAccountOptions>>(),
                    services.GetService<ILogger<TerraBatchPoolManager>>());
            }

            logger.LogInformation("Using default Batch Pool Manager.");

            return ActivatorUtilities.CreateInstance<ArmBatchPoolManager>(services);
        }

        private IRepository<TesTask> CreateCosmosDbRepositoryFromConfiguration(IServiceProvider services)
        {
            var options = services.GetRequiredService<IOptions<CosmosDbOptions>>();

            if (!string.IsNullOrWhiteSpace(options.Value.CosmosDbKey))
            {
                return WrapService(ActivatorUtilities.CreateInstance<CosmosDbRepository<TesTask>>(services,
                    options.Value.CosmosDbEndpoint, options.Value.CosmosDbKey, CosmosDbDatabaseId, CosmosDbContainerId, CosmosDbPartitionId));
            }

            var azureProxy = services.GetRequiredService<IAzureProxy>();

            (var cosmosDbEndpoint, var cosmosDbKey) = azureProxy.GetCosmosDbEndpointAndKeyAsync(options.Value.AccountName).Result;

            return WrapService(ActivatorUtilities.CreateInstance<CosmosDbRepository<TesTask>>(services,
                cosmosDbEndpoint, cosmosDbKey, CosmosDbDatabaseId, CosmosDbContainerId, CosmosDbPartitionId));

            IRepository<TesTask> WrapService(IRepository<TesTask> service)
                => ActivatorUtilities.CreateInstance<CachingWithRetriesRepository<TesTask>>(services, service);
        }

        private IStorageAccessProvider CreateStorageAccessProviderFromConfiguration(IServiceProvider services)
        {
            var options = services.GetRequiredService<IOptions<TerraOptions>>();

            logger.LogInformation("Attempting to create a Storage Access Provider");

            //if workspace id is set, then we are assuming we are running in terra
            if (!string.IsNullOrEmpty(options.Value.WorkspaceId))
            {
                logger.LogInformation("Terra Workspace Id is set. Using Terra Storage Provider");

                ValidateRequiredOptionsForTerraStorageProvider(options.Value);

                return new TerraStorageAccessProvider(
                    services.GetRequiredService<ILogger<TerraStorageAccessProvider>>(),
                    options,
                    services.GetRequiredService<IAzureProxy>(),
                    ActivatorUtilities.CreateInstance<TerraWsmApiClient>(services));
            }

            logger.LogInformation("Using Default Storage Provider");

            return ActivatorUtilities.CreateInstance<DefaultStorageAccessProvider>(services);
        }

        private void ValidateRequiredOptionsForTerraStorageProvider(TerraOptions terraOptions)
        {
            ArgumentException.ThrowIfNullOrEmpty(terraOptions.WorkspaceId, nameof(terraOptions.WorkspaceId));
            ArgumentException.ThrowIfNullOrEmpty(terraOptions.WorkspaceStorageAccountName, nameof(terraOptions.WorkspaceStorageAccountName));
            ArgumentException.ThrowIfNullOrEmpty(terraOptions.WorkspaceStorageContainerName, nameof(terraOptions.WorkspaceStorageContainerName));
            ArgumentException.ThrowIfNullOrEmpty(terraOptions.WorkspaceStorageContainerResourceId, nameof(terraOptions.WorkspaceStorageContainerResourceId));
            ArgumentException.ThrowIfNullOrEmpty(terraOptions.LandingZoneApiHost, nameof(terraOptions.WsmApiHost));
        }

        private BatchAccountResourceInformation CreateBatchAccountResourceInformation(IServiceProvider services)
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
                var task = ArmResourceInformationFinder.TryGetResourceInformationFromAccountNameAsync(options.Value.AccountName);
                task.Wait();

                if (task.Result == null)
                {
                    throw new InvalidOperationException(
                        $"Failed to get the resource information for the Batch account using ARM. Please check the options provided. Provided Batch account name:{options.Value.AccountName}");
                }

                return task.Result;
            }

            //assume the information was provided via configuration
            return new BatchAccountResourceInformation(options.Value.AccountName, options.Value.ResourceGroup, options.Value.SubscriptionId, options.Value.Region);
        }

        /// <summary>
        /// This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        /// </summary>
        /// <param name="app">An Microsoft.AspNetCore.Builder.IApplicationBuilder for the app to configure.</param>
        public void Configure(IApplicationBuilder app)
        => app.UseRouting()
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
                .UseSwaggerUI(c =>
                {
                    c.SwaggerEndpoint("/swagger/0.4.0/openapi.json", "Task Execution Service");
                })

                .IfThenElse(hostingEnvironment.IsDevelopment(),
                    s =>
                    {
                        var r = s.UseDeveloperExceptionPage();
                        logger.LogInformation("Configuring for Development environment");
                        return r;
                    },
                    s =>
                    {
                        var r = s.UseHsts();
                        logger.LogInformation("Configuring for Production environment");
                        return r;
                    });
    }

    internal static class BooleanMethodSelectorExtensions
    {
        public static IApplicationBuilder IfThenElse(this IApplicationBuilder builder, bool @if, Func<IApplicationBuilder, IApplicationBuilder> then, Func<IApplicationBuilder, IApplicationBuilder> @else)
            => @if ? then(builder) : @else(builder);

        public static IServiceCollection IfThenElse(this IServiceCollection services, bool @if, Func<IServiceCollection, IServiceCollection> then, Func<IServiceCollection, IServiceCollection> @else)
            => @if ? then(services) : @else(services);
    }
}
