// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Tes.Runner;
using Tes.Runner.Authentication;
using Tes.Runner.Docker;
using Tes.Runner.Events;
using Tes.Runner.Logs;
using Tes.Runner.Storage;
using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands
{
    public class Services
    {
        public const string LogLevelEnvVariableName = "RUNNER_LOG_LEVEL";

        internal static void ConfigureConsoleLogger(ILoggingBuilder builder)
        {
            builder.AddFilter("Microsoft.Hosting.Lifetime", LogLevel.Error);

            var logLevel = LogLevel.Information;

            if (Enum.TryParse<LogLevel>(Environment.GetEnvironmentVariable(LogLevelEnvVariableName), out var userLevel))
            {
                logLevel = userLevel;
            }

            builder.AddSimpleConsole(options =>
            {
                options.IncludeScopes = true;
                options.TimestampFormat = "yyyy-MM-dd HH:mm:ss.fff ";
                options.UseUtcTimestamp = true;
            }).SetMinimumLevel(logLevel);
        }

        private static void ConfigureParametersCore(IHostApplicationBuilder builder, Runner.Models.RuntimeOptions runtimeOptions, string apiVersion)
            => builder.Services
                .AddSingleton(runtimeOptions)
                .AddKeyedSingleton(Executor.ApiVersion, apiVersion)
                .AddSingleton(runtimeOptions.Terra ?? new())
                .AddSingleton(runtimeOptions.AzureEnvironmentConfig ?? new())
            ;

        internal static Action<IHostApplicationBuilder> ConfigureParameters(Runner.Models.RuntimeOptions runtimeOptions, string apiVersion)
            => new(builder => ConfigureParametersCore(builder, runtimeOptions, apiVersion));

        internal static Action<IHostApplicationBuilder> ConfigureParameters(Runner.Models.NodeTask nodeTask, string apiVersion)
            => new(builder =>
            {
                builder.Services
                    .AddSingleton(nodeTask)
                ;

                ConfigureParametersCore(builder, nodeTask.RuntimeOptions, apiVersion);
            });

        internal static T Create<T>(Func<ILogger<T>, T> factory)
            => factory(LoggerFactory.Create(ConfigureConsoleLogger).CreateLogger<T>());

        internal static Task<TResult> BuildAndRunAsync<TService, TResult>(Func<TService, Task<TResult>> task, Action<IHostApplicationBuilder>? configure = default) where TService : notnull
            => BuildAndRunImplAsync(provider => task(provider.GetRequiredService<TService>()), configure);

        private static async Task<T> BuildAndRunImplAsync<T>(Func<IServiceProvider, Task<T>> task, Action<IHostApplicationBuilder>? configure = default)
        {
            var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings());
            ConfigureConsoleLogger(builder.Logging);
            Configure(builder.Services, builder.Configuration);
            configure?.Invoke(builder);
            var host = builder.Build();

            try
            {
                await host.StartAsync();

                return await task(host.Services);
            }
            finally
            {
                await host.StopAsync();
            }
        }

        private static Func<IServiceProvider, Lazy<T>> LazyFactory<T>(Func<IServiceProvider, T> factory, LazyThreadSafetyMode mode)
            => provider => new Lazy<T>(() => factory(provider), mode);

        private static Func<IServiceProvider, object?, Lazy<T>> LazyKeyedFactory<T>(Func<IServiceProvider, T> factory, LazyThreadSafetyMode mode)
            => (provider, _) => new Lazy<T>(() => factory(provider), mode);

        private static Func<IServiceProvider, Func<TArgA, TService>> GetFactory<TService, TArgA>() =>
            new(provider => new((argA) => ActivatorUtilities.CreateFactory<TService>([typeof(TArgA)])(provider, [argA])));

        private static Func<IServiceProvider, Func<TArgA, TArgB, TService>> GetFactory<TService, TArgA, TArgB>() =>
            new(provider => new((argA, argB) => ActivatorUtilities.CreateFactory<TService>([typeof(TArgA), typeof(TArgB)])(provider, [argA, argB])));

        private static Func<IServiceProvider, Func<TArgA, TArgB, TArgC, TArgD, TService>> GetFactory<TService, TArgA, TArgB, TArgC, TArgD>() =>
            new(provider => new((argA, argB, argC, argD) => ActivatorUtilities.CreateFactory<TService>([typeof(TArgA), typeof(TArgB), typeof(TArgC), typeof(TArgD)])(provider, [argA, argB, argC, argD])));

        private static void Configure(IServiceCollection services, IConfigurationManager configuration)
        {
            services
                .AddSingleton(GetFactory<AppendBlobLogPublisher, Uri, string>())
                .AddSingleton(GetFactory<BlobDownloader, BlobPipelineOptions, Channel<byte[]>>())
                .AddSingleton(GetFactory<BlobStorageEventSink, Uri>())
                .AddSingleton(GetFactory<BlobUploader, BlobPipelineOptions, Channel<byte[]>>())
                .AddSingleton(GetFactory<DockerExecutor, Uri>())
                .AddSingleton(GetFactory<EventsPublisher, IList<IEventSink>>())
                .AddSingleton(GetFactory<NodeTaskResolver.NodeTaskDownloader, ILogger>())
                .AddSingleton(GetFactory<PartsProducer, IBlobPipeline, BlobPipelineOptions>())
                .AddSingleton(GetFactory<PartsWriter, IBlobPipeline, BlobPipelineOptions, Channel<byte[]>, IScalingStrategy>())
                .AddSingleton(GetFactory<ProcessedPartsProcessor, IBlobPipeline>())
                .AddSingleton(GetFactory<ProcessLauncher, IStreamLogReader>())

                .AddSingleton<Func<Runner.Models.RuntimeOptions, string, ResolutionPolicyHandler>>(provider =>
                    new((runtime, version) => ActivatorUtilities.CreateFactory<ResolutionPolicyHandler>([typeof(UrlTransformationStrategyFactory)])
                        .Invoke(provider, [ActivatorUtilities.CreateFactory<UrlTransformationStrategyFactory>([typeof(Runner.Models.RuntimeOptions), typeof(string)])
                            .Invoke(provider, [runtime, version])])))
                .AddSingleton<Func<Runner.Models.RuntimeOptions, Azure.Core.TokenCredential>>(provider =>
                    new(runtimeOptions => provider.GetRequiredService<CredentialsManager>().GetTokenCredential(runtimeOptions)))
                .AddSingleton<Func<Runner.Models.RuntimeOptions, string, Azure.Core.TokenCredential>>(provider =>
                    new((runtimeOptions, tokenScope) => provider.GetRequiredService<CredentialsManager>().GetTokenCredential(runtimeOptions, tokenScope)))

                .AddSingleton<CommandLauncher>()
                .AddSingleton<CommandHandlers>()
                .AddSingleton<ContainerRegistryAuthorizationManager>()
                .AddSingleton<CredentialsManager>()
                .AddSingleton<Executor>()
                .AddSingleton<IFileInfoProvider, DefaultFileInfoProvider>()
                .AddSingleton<FileOperationResolver>()
                .AddSingleton<LogPublisher>()
                .AddSingleton<NetworkUtility>()
                .AddSingleton<ProcessLauncherFactory>()
                .AddSingleton<ISystemInfoProvider, LinuxSystemInfoProvider>()
                .AddSingleton<ITransferOperationFactory, TransferOperationFactory>()
                .AddSingleton<UrlTransformationStrategyFactory>()
                .AddSingleton<VolumeBindingsGenerator>()

                .AddSingleton(LazyFactory(provider =>
                    provider.GetRequiredService<ConsoleStreamLogPublisher>(),
                    LazyThreadSafetyMode.PublicationOnly))
                .AddTransient<ConsoleStreamLogPublisher>()
                .AddSingleton(LazyFactory(provider =>
                    provider.GetRequiredService<NodeTaskResolver>(),
                    LazyThreadSafetyMode.PublicationOnly))
                .AddTransient<NodeTaskResolver>()
                .AddSingleton(LazyFactory(provider =>
                    provider.GetRequiredService<ResolutionPolicyHandler>()
                    .CreateEventsPublisherAsync(
                        provider.GetRequiredService<Runner.Models.NodeTask>(),
                        provider.GetRequiredService<Func<Uri, BlobStorageEventSink>>()),
                    LazyThreadSafetyMode.ExecutionAndPublication))
                .AddSingleton<ResolutionPolicyHandler>()
                .AddSingleton(LazyFactory(provider =>
                    provider.GetRequiredService<PipelineOptionsOptimizer>(),
                    LazyThreadSafetyMode.PublicationOnly))
                .AddTransient<PipelineOptionsOptimizer>()

                .AddKeyedSingleton(UrlTransformationStrategyFactory.CloudProvider, LazyKeyedFactory(provider =>
                    (IUrlTransformationStrategy)provider.GetRequiredService<CloudProviderSchemeConverter>(),
                    LazyThreadSafetyMode.PublicationOnly))
                .AddTransient<CloudProviderSchemeConverter>()
                .AddKeyedSingleton(UrlTransformationStrategyFactory.PassThroughUrl, LazyKeyedFactory(provider =>
                    (IUrlTransformationStrategy)provider.GetRequiredService<PassThroughUrlTransformationStrategy>(),
                    LazyThreadSafetyMode.PublicationOnly))
                .AddTransient<PassThroughUrlTransformationStrategy>()
                .AddKeyedSingleton(UrlTransformationStrategyFactory.ArmUrl, LazyKeyedFactory(provider =>
                    (IUrlTransformationStrategy)provider.GetRequiredService<ArmUrlTransformationStrategy>(),
                    LazyThreadSafetyMode.PublicationOnly))
                .AddTransient<ArmUrlTransformationStrategy>()
                .AddKeyedSingleton(UrlTransformationStrategyFactory.TerraUrl, LazyKeyedFactory(provider =>
                    (IUrlTransformationStrategy)provider.GetRequiredService<TerraUrlTransformationStrategy>(),
                    LazyThreadSafetyMode.PublicationOnly))
                .AddTransient<TerraUrlTransformationStrategy>()
                .AddKeyedSingleton(UrlTransformationStrategyFactory.DrsUrl, LazyKeyedFactory(provider =>
                    (IUrlTransformationStrategy)provider.GetRequiredService<DrsUriTransformationStrategy>(),
                    LazyThreadSafetyMode.PublicationOnly))
                .AddTransient<DrsUriTransformationStrategy>()
                ;
        }
    }
}
