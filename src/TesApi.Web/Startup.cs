// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using LazyCache;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.EntityFrameworkCore.Scaffolding.Metadata;
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
using Tes.Utilities;
using TesApi.Filters;
using TesApi.Web.Management;
using TesApi.Web.Management.Configuration;

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
        private readonly ILoggerFactory loggerFactory;
        private readonly IWebHostEnvironment hostingEnvironment;

        /// <summary>
        /// Startup class for ASP.NET core
        /// </summary>
        public Startup(IConfiguration configuration, ILoggerFactory loggerFactory, IWebHostEnvironment hostingEnvironment)
        {
            Configuration = configuration;
            this.hostingEnvironment = hostingEnvironment;
            logger = loggerFactory.CreateLogger<Startup>();
            this.loggerFactory = loggerFactory;
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
            ConfigureServicesImpl(services);
        }

        private void ConfigureServicesImpl(IServiceCollection services)
        {
            services
                .Configure<BatchAccountOptions>(Configuration.GetSection(BatchAccountOptions.BatchAccount))
                .Configure<CosmosDbOptions>(Configuration.GetSection(CosmosDbOptions.CosmosDbAccount))
                .AddSingleton<IAppCache>(s => new CachingService())
                .AddSingleton(CreateCosmosDbRepositoryFromConfiguration)
                .AddSingleton<AzureProxy, AzureProxy>()
                .AddSingleton<IAzureProxy>(s => new CachingWithRetriesAzureProxy(s.GetRequiredService<AzureProxy>(), s.GetRequiredService<IAppCache>()))
                .AddSingleton<IStorageAccessProvider, StorageAccessProvider>()


                .AddControllers()
                .AddNewtonsoftJson(opts =>
                {
                    opts.SerializerSettings.ContractResolver = new CamelCasePropertyNamesContractResolver();
                    opts.SerializerSettings.Converters.Add(new StringEnumConverter(new CamelCaseNamingStrategy()));
                }).Services

                .AddLogging()
                .AddSingleton<IBatchQuotaProvider, ArmBatchQuotaProvider>()
                .AddSingleton<IBatchQuotaVerifier, BatchQuotaVerifier>()
                .AddSingleton<IBatchScheduler, BatchScheduler>()
                .AddSingleton<PriceApiClient, PriceApiClient>()
                .AddSingleton<IBatchSkuInformationProvider, PriceApiBatchSkuInformationProvider>()
                .AddSingleton(CreateBatchAccountResourceInformation)
                .AddSingleton<AzureManagementClientsFactory, AzureManagementClientsFactory>()
                .AddSingleton<ArmBatchQuotaProvider, ArmBatchQuotaProvider>() //added so config utils gets the arm implementation, to be removed once config utils is refactored.
                .AddSingleton<ConfigurationUtils, ConfigurationUtils>() //this should not be call if running in terra.
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
                    c.IncludeXmlComments(
                        $"{AppContext.BaseDirectory}{Path.DirectorySeparatorChar}{Assembly.GetEntryAssembly().GetName().Name}.xml");
                    c.OperationFilter<GeneratePathParamsValidationFilter>();
                })

            .AddHostedService<Scheduler>()
            .AddHostedService<DeleteCompletedBatchJobsHostedService>()
            .AddHostedService<DeleteOrphanedBatchJobsHostedService>()
            .AddHostedService<DeleteOrphanedAutoPoolsHostedService>()
            .AddHostedService<RefreshVMSizesAndPricesHostedService>()
            .AddHostedService<DoOnceAtStartUpService>()


            //Configure AppInsights Azure Service when in PRODUCTION environment
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

        }


        private IRepository<TesTask> CreateCosmosDbRepositoryFromConfiguration(IServiceProvider services)
        {
            var options = services.GetRequiredService<IOptions<CosmosDbOptions>>();

            if (!string.IsNullOrWhiteSpace(options.Value.CosmosDbKey))
            {
                return new CachingWithRetriesRepository<TesTask>(
                    new CosmosDbRepository<TesTask>(options.Value.CosmosDbEndpoint, options.Value.CosmosDbKey,
                        CosmosDbDatabaseId, CosmosDbContainerId, CosmosDbPartitionId));
            }

            var azureProxy = services.GetRequiredService<IAzureProxy>();

            (var cosmosDbEndpoint, var cosmosDbKey) = azureProxy.GetCosmosDbEndpointAndKeyAsync(options.Value.AccountName).Result;

            return new CachingWithRetriesRepository<TesTask>(new CosmosDbRepository<TesTask>(cosmosDbEndpoint, cosmosDbKey,
                CosmosDbDatabaseId, CosmosDbContainerId, CosmosDbPartitionId));

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
                var task = AzureManagementClientsFactory.TryGetResourceInformationFromAccountNameAsync(options.Value
                        .AccountName);

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
                    c.SwaggerEndpoint("/swagger/0.3.3/openapi.json", "Task Execution Service");
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
