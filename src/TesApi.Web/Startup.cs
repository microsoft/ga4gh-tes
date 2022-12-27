﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using LazyCache;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;
using Tes.Models;
using Tes.Repository;
using TesApi.Filters;

namespace TesApi.Web
{
    /// <summary>
    /// Startup
    /// </summary>
    public class Startup
    {
        private const string DefaultAzureOfferDurableId = "MS-AZR-0003p";
        private const string CosmosDbDatabaseId = "TES";
        private const string CosmosDbContainerId = "Tasks";
        private const string CosmosDbPartitionId = "01";

        private readonly ILogger logger;
        private readonly ILoggerFactory loggerFactory;
        private readonly IWebHostEnvironment hostingEnvironment;
        private readonly string azureOfferDurableId;
        private static readonly string postgresConnectionString =
            $"Server={Environment.GetEnvironmentVariable("PostgreSqlServerName")}; " +
            $"User Id={Environment.GetEnvironmentVariable("PostgreSqlTesUserLogin")}.postgres.database.azure.com; " +
            $"Database={Environment.GetEnvironmentVariable("PostgreSqlTesDatabaseName")}; " +
            $"Port={Environment.GetEnvironmentVariable("PostgreSqlTesDatabasePort")}; " +
            $"Password={Environment.GetEnvironmentVariable("PostgreSqlTesUserPassword")}; " +
            $"SSLMode=Prefer";

        static readonly Func<RepositoryDb> createDbContext = () =>
        {
            return new RepositoryDb(postgresConnectionString);
        };

        private static async Task InitializeDbAsync()
        {
            using var dbContext = createDbContext();
            await dbContext.Database.EnsureCreatedAsync();
        }

        /// <summary>
        /// Startup class for ASP.NET core
        /// </summary>
        public Startup(IConfiguration configuration, ILoggerFactory loggerFactory, IWebHostEnvironment hostingEnvironment)
        {
            Configuration = configuration;
            this.hostingEnvironment = hostingEnvironment;
            logger = loggerFactory.CreateLogger<Startup>();
            this.loggerFactory = loggerFactory;
            azureOfferDurableId = Configuration.GetValue("AzureOfferDurableId", DefaultAzureOfferDurableId);
        }

        /// <summary>
        /// The application configuration
        /// </summary>
        public IConfiguration Configuration { get; }

        /// <summary>
        /// This method gets called by the runtime. Use this method to add services to the container.
        /// </summary>
        /// <param name="services">The Microsoft.Extensions.DependencyInjection.IServiceCollection to add the services to.</param>
        public void ConfigureServices(IServiceCollection services)
        {
            var (cache, azureProxy, cachingAzureProxy, storageAccessProvider, repository) = ConfigureServices();
            ConfigureServices(services, cache, azureProxy, cachingAzureProxy, storageAccessProvider, repository);
        }

        private (IAppCache cache, AzureProxy azureProxy, IAzureProxy cachingAzureProxy, IStorageAccessProvider storageAccessProvider, IRepository<TesTask> repository) ConfigureServices()
        {
            var cache = new CachingService();
            var postgreSqlServerName = Environment.GetEnvironmentVariable("PostgreSqlServerName");

            var azureProxy = new AzureProxy(Configuration["BatchAccountName"], azureOfferDurableId, loggerFactory.CreateLogger<AzureProxy>());
            IAzureProxy cachingAzureProxy = new CachingWithRetriesAzureProxy(azureProxy, cache);
            IStorageAccessProvider storageAccessProvider = new StorageAccessProvider(loggerFactory.CreateLogger<StorageAccessProvider>(), Configuration, cachingAzureProxy);

            var configurationUtils = new ConfigurationUtils(Configuration, cachingAzureProxy, storageAccessProvider, loggerFactory.CreateLogger<ConfigurationUtils>());
            configurationUtils.ProcessAllowedVmSizesConfigurationFileAsync().Wait();

            (var cosmosDbEndpoint, var cosmosDbKey) = (postgreSqlServerName is null) ? azureProxy.GetCosmosDbEndpointAndKeyAsync(Configuration["CosmosDbAccountName"]).Result : (null, null);

            if (postgreSqlServerName is not null)
            {
                InitializeDbAsync().Wait();
            }

            IRepository<TesTask> database = (postgreSqlServerName is null)
                ? new CosmosDbRepository<TesTask>(
                    cosmosDbEndpoint,
                    cosmosDbKey,
                    CosmosDbDatabaseId,
                    CosmosDbContainerId,
                    CosmosDbPartitionId)
                : new TesTaskPostgreSqlRepository(createDbContext);

            return (cache, azureProxy, cachingAzureProxy, storageAccessProvider, database);
        }

        private void ConfigureServices(IServiceCollection services, IAppCache cache, AzureProxy azureProxy, IAzureProxy cachingAzureProxy, IStorageAccessProvider storageAccessProvider, IRepository<TesTask> repository)
            => services.AddSingleton(cache)

                .AddSingleton(cachingAzureProxy)
                .AddSingleton(azureProxy)

                .AddControllers()
                .AddNewtonsoftJson(opts =>
                {
                    opts.SerializerSettings.ContractResolver = new CamelCasePropertyNamesContractResolver();
                    opts.SerializerSettings.Converters.Add(new StringEnumConverter(new CamelCaseNamingStrategy()));
                }).Services

                .AddSingleton<IRepository<TesTask>>(new CachingWithRetriesRepository<TesTask>(repository))
                .AddSingleton<IBatchScheduler>(new BatchScheduler(loggerFactory.CreateLogger<BatchScheduler>(), Configuration, cachingAzureProxy, storageAccessProvider))

                .AddSwaggerGen(c =>
                {
                    c.SwaggerDoc("0.3.3", new OpenApiInfo
                    {
                        Version = "0.3.3",
                        Title = "Task Execution Service",
                        Description = "Task Execution Service (ASP.NET Core 7.0)",
                        Contact = new OpenApiContact()
                        {
                            Name = "Microsoft Biomedical Platforms and Genomics",
                            Url = new Uri("https://github.com/microsoft/CromwellOnAzure")
                        },
                    });
                    c.CustomSchemaIds(type => type.FullName);
                    c.IncludeXmlComments($"{AppContext.BaseDirectory}{Path.DirectorySeparatorChar}{Assembly.GetEntryAssembly().GetName().Name}.xml");
                    c.OperationFilter<GeneratePathParamsValidationFilter>();
                })

                .AddHostedService<Scheduler>()
                .AddHostedService<DeleteCompletedBatchJobsHostedService>()
                .AddHostedService<DeleteOrphanedBatchJobsHostedService>()
                .AddHostedService<DeleteOrphanedAutoPoolsHostedService>()
                .AddHostedService<RefreshVMSizesAndPricesHostedService>()

            // Configure AppInsights Azure Service when in PRODUCTION environment
                .IfThenElse(hostingEnvironment.IsProduction(),
                    s =>
                    {
                        var applicationInsightsAccountName = Configuration["ApplicationInsightsAccountName"];
                        var instrumentationKey = AzureProxy.GetAppInsightsInstrumentationKeyAsync(applicationInsightsAccountName).Result;

                        if (instrumentationKey is not null)
                        {
                            var connectionString = $"InstrumentationKey={instrumentationKey}";
                            return s.AddApplicationInsightsTelemetry(options =>
                                {
                                    options.ConnectionString = connectionString;
                                });
                        }

                        return s;
                    },
                    s => s.AddApplicationInsightsTelemetry());

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
                    c.SwaggerEndpoint("/swagger/0.3.0/openapi.json", "Task Execution Service");
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
