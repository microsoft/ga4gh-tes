// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Tes.Runner.Authentication;
using Tes.Runner.Docker;
using Tes.Runner.Events;
using Tes.Runner.Logs;
using Tes.Runner.Storage;
using Tes.Runner.Transfer;
using Tes.Runner;

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

        private static ObjectFactory<TService> GetImplementationFactory<TService, TArgA>()
            => ActivatorUtilities.CreateFactory<TService>([typeof(TArgA)]);

        private static Func<IServiceProvider, Func<TArgA, TService>> GetFactory<TService, TArgA>(ObjectFactory<TService> objectFactory)
            => new(provider => new((argA) => objectFactory(provider, [argA])));

        private static ObjectFactory<TService> GetImplementationFactory<TService, TArgA, TArgB>() =>
            ActivatorUtilities.CreateFactory<TService>([typeof(TArgA), typeof(TArgB)]);

        private static Func<IServiceProvider, Func<TArgA, TArgB, TService>> GetFactory<TService, TArgA, TArgB>(ObjectFactory<TService> objectFactory)
            => new(provider => new((argA, argB) => objectFactory(provider, [argA, argB])));

        private static ObjectFactory<TService> GetImplementationFactory<TService, TArgA, TArgB, TArgC, TArgD>() =>
            ActivatorUtilities.CreateFactory<TService>([typeof(TArgA), typeof(TArgB), typeof(TArgC), typeof(TArgD)]);

        private static Func<IServiceProvider, Func<TArgA, TArgB, TArgC, TArgD, TService>> GetFactory<TService, TArgA, TArgB, TArgC, TArgD>(ObjectFactory<TService> objectFactory)
            => new(provider => new((argA, argB, argC, argD) => objectFactory(provider, [argA, argB, argC, argD])));

        private static void Configure(IServiceCollection services, IConfigurationManager configuration)
        {
            {
                var factory = GetImplementationFactory<AppendBlobLogPublisher, Uri, string>();
                services.AddSingleton(GetFactory<AppendBlobLogPublisher, Uri, string>(factory));
            }

            {
                var factory = GetImplementationFactory<BlobDownloader, BlobPipelineOptions, Channel<byte[]>>();
                services.AddSingleton(GetFactory<BlobDownloader, BlobPipelineOptions, Channel<byte[]>>(factory));
            }

            {
                var factory = GetImplementationFactory<BlobStorageEventSink, Uri>();
                services.AddSingleton(GetFactory<BlobStorageEventSink, Uri>(factory));
            }

            {
                var factory = GetImplementationFactory<BlobUploader, BlobPipelineOptions, Channel<byte[]>>();
                services.AddSingleton(GetFactory<BlobUploader, BlobPipelineOptions, Channel<byte[]>>(factory));
            }

            {
                var factory = GetImplementationFactory<DockerExecutor, Uri>();
                services.AddSingleton(GetFactory<DockerExecutor, Uri>(factory));
            }

            {
                var factory = GetImplementationFactory<EventsPublisher, IList<IEventSink>>();
                services.AddSingleton(GetFactory<EventsPublisher, IList<IEventSink>>(factory));
            }

            {
                var factory = GetImplementationFactory<PartsProducer, IBlobPipeline, BlobPipelineOptions>();
                services.AddSingleton(GetFactory<PartsProducer, IBlobPipeline, BlobPipelineOptions>(factory));
            }

            {
                var factory = GetImplementationFactory<PartsWriter, IBlobPipeline, BlobPipelineOptions, Channel<byte[]>, IScalingStrategy>();
                services.AddSingleton(GetFactory<PartsWriter, IBlobPipeline, BlobPipelineOptions, Channel<byte[]>, IScalingStrategy>(factory));
            }

            {
                var factory = GetImplementationFactory<ProcessedPartsProcessor, IBlobPipeline>();
                services.AddSingleton(GetFactory<ProcessedPartsProcessor, IBlobPipeline>(factory));
            }

            {
                var factory = GetImplementationFactory<ProcessLauncher, IStreamLogReader>();
                services.AddSingleton(GetFactory<ProcessLauncher, IStreamLogReader>(factory));
            }

            services
                .AddSingleton<ArmUrlTransformationStrategy>()
                .AddSingleton<CloudProviderSchemeConverter>()
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
