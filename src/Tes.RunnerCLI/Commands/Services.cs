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

        internal static Action<IHostApplicationBuilder> ConfigureParameters(Runner.Models.NodeTask nodeTask, string apiVersion)
            => new(builder => builder.Services
                .AddSingleton(nodeTask)
                .AddSingleton(nodeTask.RuntimeOptions)
                .AddKeyedSingleton(Executor.ApiVersion, apiVersion)
                .AddSingleton(nodeTask.RuntimeOptions.Terra ?? new())
                .AddSingleton(nodeTask.RuntimeOptions.AzureEnvironmentConfig ?? new())
            );

        internal static Task BuildAndRunAsync<TService>(Func<TService, Task> task, Action<IHostApplicationBuilder>? configure = default) where TService : notnull
            => BuildAndRunImplAsync(async provider => { await task(provider.GetRequiredService<TService>()); return false; }, configure);

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
                //.AddTransient<AppendBlobLogPublisher>()
                .AddSingleton(GetFactory<BlobDownloader, BlobPipelineOptions, Channel<byte[]>>())
                //.AddTransient<BlobDownloader>()
                .AddSingleton(GetFactory<BlobStorageEventSink, Uri>())
                //.AddTransient<BlobStorageEventSink>()
                .AddSingleton(GetFactory<BlobUploader, BlobPipelineOptions, Channel<byte[]>>())
                //.AddTransient<BlobUploader>()
                .AddSingleton(GetFactory<DockerExecutor, Uri>())
                //.AddTransient<DockerExecutor>()
                .AddSingleton(GetFactory<EventsPublisher, IList<IEventSink>>())
                //.AddTransient<EventsPublisher>()
                .AddSingleton(GetFactory<PartsProducer, IBlobPipeline, BlobPipelineOptions>())
                //.AddTransient<PartsProducer>()
                .AddSingleton(GetFactory<PartsWriter, IBlobPipeline, BlobPipelineOptions, Channel<byte[]>, IScalingStrategy>())
                //.AddTransient<PartsWriter>()
                .AddSingleton(GetFactory<ProcessedPartsProcessor, IBlobPipeline>())
                //.AddTransient<ProcessedPartsProcessor>()
                .AddSingleton(GetFactory<ProcessLauncher, IStreamLogReader>())
                //.AddTransient<ProcessLauncher>()

                .AddSingleton<ArmUrlTransformationStrategy>()
                .AddSingleton<CloudProviderSchemeConverter>()
                .AddSingleton<CommandLauncher>()
                .AddSingleton<CommandHandlers>()
                .AddSingleton<ConsoleStreamLogPublisher>()
                .AddSingleton<ContainerRegistryAuthorizationManager>()
                .AddSingleton<CredentialsManager>()
                .AddSingleton<DrsUriTransformationStrategy>()
                .AddSingleton<Executor>()
                .AddSingleton<IFileInfoProvider, DefaultFileInfoProvider>()
                .AddSingleton<FileOperationResolver>()
                .AddSingleton<LogPublisher>()
                .AddSingleton<NetworkUtility>()
                .AddSingleton<PassThroughUrlTransformationStrategy>()
                .AddSingleton<PipelineOptionsOptimizer>()
                .AddSingleton<ProcessLauncherFactory>()
                .AddSingleton<ResolutionPolicyHandler>()
                .AddSingleton<ISystemInfoProvider, LinuxSystemInfoProvider>()
                .AddSingleton<TerraUrlTransformationStrategy>()
                .AddSingleton<ITransferOperationFactory, TransferOperationFactory>()
                .AddSingleton<UrlTransformationStrategyFactory>()
                .AddSingleton<VolumeBindingsGenerator>()

                .AddSingleton(LazyFactory(provider =>
                    provider.GetRequiredService<ResolutionPolicyHandler>()
                    .CreateEventsPublisherAsync(
                        provider.GetRequiredService<Runner.Models.NodeTask>(),
                        provider.GetRequiredService<Func<Uri, BlobStorageEventSink>>()),
                    LazyThreadSafetyMode.ExecutionAndPublication))
                .AddSingleton(LazyFactory(provider =>
                    provider.GetRequiredService<PipelineOptionsOptimizer>(),
                    LazyThreadSafetyMode.PublicationOnly))

                .AddKeyedSingleton(UrlTransformationStrategyFactory.CloudProvider,
                    LazyKeyedFactory(provider => (IUrlTransformationStrategy)provider.GetRequiredService<CloudProviderSchemeConverter>(), LazyThreadSafetyMode.PublicationOnly))
                .AddKeyedSingleton(UrlTransformationStrategyFactory.PassThroughUrl,
                    LazyKeyedFactory(provider => (IUrlTransformationStrategy)provider.GetRequiredService<PassThroughUrlTransformationStrategy>(), LazyThreadSafetyMode.PublicationOnly))
                .AddKeyedSingleton(UrlTransformationStrategyFactory.ArmUrl,
                    LazyKeyedFactory(provider => (IUrlTransformationStrategy)provider.GetRequiredService<ArmUrlTransformationStrategy>(), LazyThreadSafetyMode.PublicationOnly))
                .AddKeyedSingleton(UrlTransformationStrategyFactory.TerraUrl,
                    LazyKeyedFactory(provider => (IUrlTransformationStrategy)provider.GetRequiredService<TerraUrlTransformationStrategy>(), LazyThreadSafetyMode.PublicationOnly))
                .AddKeyedSingleton(UrlTransformationStrategyFactory.DrsUrl,
                    LazyKeyedFactory(provider => (IUrlTransformationStrategy)provider.GetRequiredService<DrsUriTransformationStrategy>(), LazyThreadSafetyMode.PublicationOnly))
                ;
        }
    }
}
