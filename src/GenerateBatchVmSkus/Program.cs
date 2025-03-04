// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Immutable;
using System.CommandLine;
using System.CommandLine.Builder;
using System.CommandLine.Hosting;
using System.CommandLine.IO;
using System.CommandLine.Parsing;
using System.CommandLine.Rendering;
using System.Globalization;
using System.Reflection;
using Azure.Core;
using Azure.Identity;
using Azure.ResourceManager;
using Azure.ResourceManager.Batch;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Tes.Models;

namespace GenerateBatchVmSkus
{
    internal class Configuration
    {
        public string? SubscriptionId { get; set; }
        public string? VmOutputFilePath { get; set; }
        public string? DiskOutputFilePath { get; set; }
        public string[]? BatchAccounts { get; set; }
        public string[]? SubnetIds { get; set; }

        public static Configuration BuildConfiguration()
        {
            var configBuilder = new ConfigurationBuilder();

            var configurationSource = configBuilder
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json")
                .Build();
            var configurationProperties = typeof(Configuration).GetTypeInfo().DeclaredProperties.Select(p => p.Name).ToList();

            var invalidArguments = configurationSource.Providers
                .SelectMany(p => p.GetChildKeys([], null))
                .Where(k => !configurationProperties.Contains(k, StringComparer.OrdinalIgnoreCase));

            if (invalidArguments.Any())
            {
                throw new ArgumentException($"Invalid argument(s): {string.Join(", ", invalidArguments)}");
            }

            var configuration = new Configuration();
            configurationSource.Bind(configuration);

            return configuration;
        }
    }

    internal static class Program
    {
        private static string? _cloudName;

        static Program()
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.DefaultThreadCurrentCulture = CultureInfo.InvariantCulture;
        }

        internal static RootCommand RootCommand { get; } = new RootCommand("Determine TES Azure Compute SKU whitelist.");

        internal static Option<FileInfo> NoValidateWhitelistFile { get; } = new Option<FileInfo>(aliases: ["-V"], "--no-validation")
        {
            Arity = ArgumentArity.ExactlyOne,
            Description = "Suppress validation. Use the following whitelist. (a file spec is required if this option is selected)"
        };

        internal static Option<string> CloudName { get; } = new Option<string>(aliases: ["-c"], "--azure-cloud")
        {
            Arity = ArgumentArity.ExactlyOne,
            Description = "Short name of the azure cloud.",
            IsRequired = true,
        };

        private static CommandLineBuilder BuildHostBuilder()
        {
            RootCommand.AddOption(NoValidateWhitelistFile);
            RootCommand.AddOption(CloudName);
            CloudName.FromAmong([nameof(AzureAuthorityHosts.AzurePublicCloud), nameof(AzureAuthorityHosts.AzureGovernment), nameof(AzureAuthorityHosts.AzureChina)]);
            CloudName.SetDefaultValue(nameof(AzureAuthorityHosts.AzurePublicCloud));

            CommandLineBuilder builder = new(RootCommand);

            builder.UseDefaults()
                .UseHost(Host.CreateDefaultBuilder, builder =>
                {
                    builder.ConfigureHostOptions(options => options.ShutdownTimeout = TimeSpan.FromSeconds(60))

                    .ConfigureAppConfiguration((context, config) => config.AddJsonFile("appsettings.json"))

                    .ConfigureLogging((context, logging) =>
                        logging.AddConsole(options => options.LogToStandardErrorThreshold = LogLevel.Warning)

                        .AddSystemdConsole(options =>
                        {
                            options.IncludeScopes = true;
                            options.UseUtcTimestamp = true;
                        })

                        .SetMinimumLevel(LogLevel.Information)
                        .AddFilter("Azure.Core", LogLevel.Error)
                        .AddFilter("Azure.Identity", LogLevel.Warning)
                        .Configure(options => options.ActivityTrackingOptions = ActivityTrackingOptions.ParentId)
                        .AddConfiguration(context.Configuration))

                    .ConfigureServices((context, services) =>
                    {
                        services.AddSingleton(Configuration.BuildConfiguration());
                        services.AddOptions<CommonUtilities.Options.RetryPolicyOptions>();

                        services.AddSingleton(s
                            => new Func<BatchAccountData, string, int, CancellationToken, BatchAccountInfo>((data, subnetId, index, token)
                                => ActivatorUtilities.CreateInstance<BatchAccountInfo>(s, data, subnetId, index, token)));

                        services.AddSingleton(_
                            => new Func<BatchAccountDataAndTokenProvider, Lazy<Microsoft.Azure.Batch.BatchClient>>(provider
                                => new(()
                                    => Microsoft.Azure.Batch.BatchClient.Open(
                                        new Microsoft.Azure.Batch.Auth.BatchTokenCredentials(
                                            new UriBuilder(Uri.UriSchemeHttps, provider.Data.AccountEndpoint).Uri.AbsoluteUri,
                                            provider.Provider)))));

                        services.AddSingleton<AzureCloud>(_ => _cloudName switch
                        {
                            nameof(ArmEnvironment.AzurePublicCloud) => new(ArmEnvironment.AzurePublicCloud, AzureAuthorityHosts.AzurePublicCloud),
                            nameof(ArmEnvironment.AzureGovernment) => new(ArmEnvironment.AzureGovernment, AzureAuthorityHosts.AzureGovernment),
                            nameof(ArmEnvironment.AzureChina) => new(ArmEnvironment.AzureChina, AzureAuthorityHosts.AzureChina),
                            _ => throw new ArgumentOutOfRangeException(nameof(CloudName)),
                        });

                        services.AddSingleton<Azure.Core.Pipeline.RetryPolicy>(s =>
                        {
                            var retryPolicy = s.GetRequiredService<Microsoft.Extensions.Options.IOptions<CommonUtilities.Options.RetryPolicyOptions>>();
                            return new(retryPolicy.Value.MaxRetryCount, DelayStrategy.CreateExponentialDelayStrategy(TimeSpan.FromSeconds(retryPolicy.Value.ExponentialBackOffExponent)));
                        });

                        services.AddSingleton<TokenCredential>(s => s.GetRequiredService<AzureCliCredential>());
                        services.AddSingleton<AzureCliCredential>(s => new(SetOptions<AzureCliCredentialOptions>(s, new())));

                        services.AddAzureClientsCore(true);
                        services.AddAzureClients(configure =>
                        {
                            configure.UseCredential(s => s.GetRequiredService<TokenCredential>());

                            configure.AddArmClient(context.Configuration[nameof(Configuration.SubscriptionId)])
                                .ConfigureOptions((o, s) =>
                                {
                                    o.Environment = s.GetRequiredService<AzureCloud>().ArmEnvironment;
                                    o.SetApiVersionsFromProfile(AzureStackProfile.Profile20200901Hybrid);
                                    o.RetryPolicy = s.GetRequiredService<Azure.Core.Pipeline.RetryPolicy>();
                                });

                            configure.AddClient<Lazy<Microsoft.Azure.Batch.BatchClient>, BatchAccountDataAndTokenProvider>((BatchAccountDataAndTokenProvider dataAndProvider, IServiceProvider s) => new(() =>
                            {
                                var (data, provider) = dataAndProvider;
                                var result = Microsoft.Azure.Batch.BatchClient.Open(
                                    new Microsoft.Azure.Batch.Auth.BatchTokenCredentials(
                                        new UriBuilder(Uri.UriSchemeHttps, data.AccountEndpoint).Uri.AbsoluteUri,
                                        provider));

                                result.CustomBehaviors.OfType<Microsoft.Azure.Batch.RetryPolicyProvider>().Single().Policy = new Microsoft.Azure.Batch.Common.ExponentialRetry(TimeSpan.FromSeconds(1), 11, TimeSpan.FromMinutes(1));
                                return result;
                            }));
                        });

                        static T SetOptions<T>(IServiceProvider s, T o) where T : TokenCredentialOptions
                        {
                            var retryPolicy = s.GetRequiredService<Microsoft.Extensions.Options.IOptions<CommonUtilities.Options.RetryPolicyOptions>>();
                            o.AuthorityHost = s.GetRequiredService<AzureCloud>().AzureAuthorityHost;
                            o.Retry.Mode = RetryMode.Exponential;
                            o.Retry.MaxRetries = retryPolicy.Value.MaxRetryCount;
                            o.Retry.Delay = TimeSpan.FromSeconds(retryPolicy.Value.ExponentialBackOffExponent);
                            return o;
                        };

                        //static T SetOrDefault<T>(string? test, Func<string, T> apply, T @default)
                        //    => string.IsNullOrEmpty(test) ? @default : apply(test);
                    })

                    .UseInvocationLifetime(builder.GetInvocationContext(), options => options.SuppressStatusMessages = true)

                    .UseCommandHandler<RootCommand, AzureBatchSkuLocator>();
                });

            builder.UseExceptionHandler(onException: (exception, context) =>
            {
                if (exception is not OperationCanceledException)
                {
                    var terminal = context.Console.GetTerminal();

                    terminal?.ResetColor();
                    terminal?.Render(ForegroundColorSpan.Red());

                    context.Console.Error.WriteLine(context.LocalizationResources.ExceptionHandlerHeader() + exception.ToString());

                    terminal?.ResetColor();
                }

                context.ExitCode = 1;
            });

            return builder;
        }

        internal record class AzureCloud(ArmEnvironment ArmEnvironment, Uri AzureAuthorityHost);
        internal record class BatchAccountDataAndTokenProvider(BatchAccountData Data, Func<Task<string>> Provider);

        static async Task Main(string[] args)
        {
            try
            {
                var results = BuildHostBuilder().Build().Parse(args);
                _cloudName = results.CommandResult.GetValueForOption(CloudName);
                Environment.Exit(await results.InvokeAsync());
            }
            catch (Exception exception) when (exception is not System.OperationCanceledException)
            {
                Console.ResetColor();
                Console.ForegroundColor = ConsoleColor.Red;

                for (var ex = exception; ex is not null; ex = ex.InnerException)
                {
                    Console.WriteLine(ex.GetType().FullName + " " + ex.Message);

                    if (ex.StackTrace is not null)
                    {
                        Console.WriteLine(ex.StackTrace);
                    }
                }

                Console.ResetColor();

                var exitCode = exception.HResult;
                if (exitCode == 0) { exitCode = 1; }
                if (Environment.OSVersion.Platform == PlatformID.Unix) { exitCode &= 0x0ff; }
                Environment.Exit(exitCode);
            }
        }

        internal record VmSku(string Name, IEnumerable<VirtualMachineInformation> Skus)
        {
            public static VmSku Create(IGrouping<string, VirtualMachineInformation> grouping)
            {
                if (!(grouping?.Any() ?? false))
                {
                    throw new ArgumentException("Each SKU must contain at least one VirtualMachineInformation.", nameof(grouping));
                }

                var sku = grouping.LastOrDefault(sku => sku.LowPriority) ?? grouping.Last();
                return new(grouping.Key, grouping) { Sku = sku };
            }

            public VirtualMachineInformation Sku { get; private set; } = Skus.Last();
        }

        internal sealed class BatchAccountInfo : IDisposable
        {
            private async Task<string> BatchTokenProvider(TokenCredential tokenCredential, CancellationToken cancellationToken)
            {
                if (!(accessToken?.ExpiresOn > DateTimeOffset.UtcNow.Subtract(TimeSpan.FromSeconds(60))))
                {
                    accessToken = await tokenCredential.GetTokenAsync(
                        new([new UriBuilder("https://batch.core.windows.net/") { Path = ".default" }.Uri.AbsoluteUri], Guid.NewGuid().ToString("D")),
                        cancellationToken);
                }

                return accessToken?.Token!;
            }

            private readonly Lazy<Microsoft.Azure.Batch.BatchClient> batchClient;
            private readonly BatchAccountData data;
            private AccessToken? accessToken;

            public BatchAccountInfo(Func<BatchAccountDataAndTokenProvider, Lazy<Microsoft.Azure.Batch.BatchClient>> batchClientFactory, TokenCredential credential, BatchAccountData data, string subnetId, int index, CancellationToken cancellationToken)
            {
                SubnetId = subnetId;
                Index = index;
                this.data = data;
                batchClient = batchClientFactory(new(data, async () => await BatchTokenProvider(credential, cancellationToken)));
            }

            public string Name => data.Name;

            public int Index { get; }

            public string SubnetId { get; }

            public AzureLocation Location => data.Location!.Value;

            public Microsoft.Azure.Batch.BatchClient Client => batchClient.Value;

            public bool? IsDedicatedCoreQuotaPerVmFamilyEnforced => data.IsDedicatedCoreQuotaPerVmFamilyEnforced;

            public IReadOnlyList<Azure.ResourceManager.Batch.Models.BatchVmFamilyCoreQuota> DedicatedCoreQuotaPerVmFamily => data.DedicatedCoreQuotaPerVmFamily;

            public int? PoolQuota => data.PoolQuota;

            public int? DedicatedCoreQuota => data.DedicatedCoreQuota;

            public int? LowPriorityCoreQuota => data.LowPriorityCoreQuota;

            void IDisposable.Dispose()
            {
                if (batchClient.IsValueCreated)
                {
                    batchClient.Value.Dispose();
                }
            }
        }
    }
}
