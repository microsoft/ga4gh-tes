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
using Azure.Identity;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace GenerateBatchVmSkus
{
    internal class Configuration
    {
        public string? SubscriptionId { get; set; }
        public string? OutputFilePath { get; set; }
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
                .SelectMany(p => p.GetChildKeys(Enumerable.Empty<string>(), null))
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

    /*
     * TODO considerations:
     *   Currently, we consider the Azure APIs to return the same values for each region where any given SKU exists, but that is not the case. Consider implementing a strategy where we, on a case-by-case basis, determine whether we want the "max" or the "min" of all the regions sampled for any given property.
     */

    internal class Program
    {
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
            IsRequired = true
        };

        CommandLineBuilder BuildHostBuilder()
        {
            var jsonFile = new Argument<FileInfo>("destination", "Minimized SKU list.") { Arity = ArgumentArity.ZeroOrOne /*Arity = ArgumentArity.ExactlyOne*/ };
            RootCommand.AddArgument(jsonFile);
            RootCommand.AddOption(NoValidateWhitelistFile);
            RootCommand.AddOption(CloudName);
            CloudName.FromAmong([nameof(AzureAuthorityHosts.AzurePublicCloud), nameof(AzureAuthorityHosts.AzureGovernment), nameof(AzureAuthorityHosts.AzureChina)]);

            CommandLineBuilder builder = new(RootCommand);

            builder.UseDefaults()
                .UseHost(Host.CreateDefaultBuilder, builder =>
                {
                    builder.ConfigureHostOptions(options => options.ShutdownTimeout = TimeSpan.FromSeconds(60))

                    .ConfigureAppConfiguration((context, config) =>
                    {
                        config.AddJsonFile("appsettings.json");
                        var cfg = config.Build();
                        var value = cfg["OutputFilePath"];
                        if (!string.IsNullOrWhiteSpace(value))
                        {
                            jsonFile.SetDefaultValue(value);
                        }
                    })

                    .ConfigureLogging((context, logging) =>
                        logging.AddConsole(options => options.LogToStandardErrorThreshold = LogLevel.Warning)

                    .AddSystemdConsole(options =>
                    {
                        options.IncludeScopes = true;
                        options.UseUtcTimestamp = true;
                    })

                    .SetMinimumLevel(LogLevel.Debug)
                    .Configure(options => options.ActivityTrackingOptions = ActivityTrackingOptions.ParentId)
                    .AddConfiguration(context.Configuration))

                    .ConfigureServices((context, services) =>
                    {
                        services.AddSingleton(Configuration.BuildConfiguration());
                        services.AddTransient<TokenCredentialOptions>();
                        services.AddTransient<AzureCliCredentialOptions>();

                        services.AddAzureClientsCore(true);
                        services.AddAzureClients(configure =>
                        {
                            //configure.AddClient<Microsoft.Azure.Batch.Protocol.BatchServiceClient, MSALTokenCredentials>((MSALTokenCredentials o, IServiceProvider s) =>
                            //{
                            //    return new(o);
                            //})
                            //.ConfigureOptions((o, s) =>
                            //{
                            //    o.Scopes = "https://batch.core.windows.net/.default";
                            //});

                            configure.AddClient<ManagedIdentityCredential, TokenCredentialOptions>((TokenCredentialOptions o, IServiceProvider s) =>
                            {
                                return new(context.Configuration["AZURE_CLIENT_ID"], o);
                            })
                            .ConfigureOptions((o, s) =>
                            {
                                o.AuthorityHost = AzureAuthorityHosts.AzurePublicCloud;
                            });

                            configure.AddClient<AzureCliCredential, AzureCliCredentialOptions>((AzureCliCredentialOptions o, IServiceProvider s) =>
                            {
                                return new(o);
                            })
                            .ConfigureOptions((o, s) =>
                            {
                                o.AuthorityHost = AzureAuthorityHosts.AzurePublicCloud;
                            });

                            configure.AddArmClient(context.Configuration)
                            .WithCredential(s => new ChainedTokenCredential(s.GetRequiredService<ManagedIdentityCredential>(), s.GetRequiredService<AzureCliCredential>()))
                            .ConfigureOptions((o, s) =>
                            {
                                o.Environment = Azure.ResourceManager.ArmEnvironment.AzurePublicCloud;
                                o.SetApiVersionsFromProfile(Azure.ResourceManager.AzureStackProfile.Profile20200901Hybrid);
                            });
                        });
                    })

                    .UseInvocationLifetime(builder.GetInvocationContext(), options => options.SuppressStatusMessages = true)

                    .UseCommandHandler<RootCommand, AzureBatchSkuLocator>();
                });

                builder.UseExceptionHandler(onException: (exception, context) =>
                {
                    if (exception is not OperationCanceledException)
                    {
                        var terminal = context.Console.GetTerminal(outputMode: context.Console.DetectOutputMode());

                        terminal.ResetColor();
                        terminal.ForegroundColor = ConsoleColor.Red;

                        context.Console.Error.Write(context.LocalizationResources.ExceptionHandlerHeader());
                        context.Console.Error.WriteLine(exception.ToString());

                        terminal.ResetColor();
                    }

                    context.ExitCode = 1;
                });

            return builder;
        }

        static async Task Main(string[] args)
        {
            try
            {
                var program = new Program();
                var builder = program.BuildHostBuilder();
                Environment.Exit(await builder.Build().Parse(args).InvokeAsync());
            }
            catch (Exception exception)
            {
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
    }
}
