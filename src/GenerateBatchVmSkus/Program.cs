// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Azure;
using Azure.Core;
using Azure.Identity;
using Azure.ResourceManager;
using Azure.ResourceManager.Batch;
using Azure.ResourceManager.Compute;
using Azure.ResourceManager.Compute.Models;
using Azure.ResourceManager.Resources.Models;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Tes.ApiClients;
using Tes.ApiClients.Models.Pricing;
using Tes.ApiClients.Options;
using Tes.Models;

namespace GenerateBatchVmSkus
{
    internal class Configuration
    {
        public string? SubscriptionId { get; set; }
        public string? OutputFilePath { get; set; }
        public string[]? BatchAccounts { get; set; }
        public string[]? SubnetIds { get; set; }

        public static Configuration BuildConfiguration(string[] args)
        {
            var configBuilder = new ConfigurationBuilder();

            var configurationSource = configBuilder
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json")
                .AddCommandLine(args)
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

    internal class Program
    {
        static Program()
        {
            Thread.CurrentThread.CurrentCulture = System.Globalization.CultureInfo.DefaultThreadCurrentCulture = System.Globalization.CultureInfo.InvariantCulture;
        }

        static async Task Main(string[] args)
        {
            Configuration? configuration = null;

            try
            {
                configuration = Configuration.BuildConfiguration(args);
            }
            catch (ArgumentException ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.Message);
                Console.ResetColor();
                Environment.Exit(2);
            }

            try
            {
                Environment.Exit(await new Program().RunAsync(configuration));
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

        private readonly ConcurrentDictionary<string, ImmutableHashSet<string>> regionsForVm = new(StringComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<string, ComputeResourceSku> skuForVm = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, PricingItem> priceForVm = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, PricingItem> lowPrPriceForVm = new(StringComparer.OrdinalIgnoreCase);
        private readonly CancellationTokenSource cancellationTokenSource = new();

        private void CancelKeyPress(object? sender, ConsoleCancelEventArgs e)
        {
            e.Cancel = true;
            cancellationTokenSource.Cancel();
        }

        private static double? ConvertMiBToGiB(int? value) => value.HasValue ? Math.Round(value.Value / 1024.0, 2) : null;

        private record struct ItemWithIndex<T>(T Item, int Index);
        private record struct ItemWithName<T>(string Name, T Item);

        private static IAsyncEnumerable<BatchAccountInfo> GetBatchAccountsAsync(Azure.ResourceManager.Resources.SubscriptionResource subscription, TokenCredential credential, Configuration configuration, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(configuration?.BatchAccounts, nameof(configuration));
            ArgumentNullException.ThrowIfNull(configuration?.SubnetIds, nameof(configuration));

            if (configuration.BatchAccounts.Length != configuration.SubnetIds.Length)
            {
                throw new ArgumentException("A subnet id is required for each batch account.", nameof(configuration));
            }

            if (configuration.SubnetIds.Distinct(StringComparer.OrdinalIgnoreCase).Count() != configuration.SubnetIds.Length)
            {
                throw new ArgumentException("Duplicate batch subnet ids provided.", nameof(configuration));
            }

            var accountsAndSubnetsInOrder = configuration.BatchAccounts.Zip(configuration.SubnetIds).Select((a, i) => new ItemWithIndex<(string BatchAccount, string SubnetId)>(a, i)).ToList();

            if (accountsAndSubnetsInOrder.Any(account => string.IsNullOrWhiteSpace(account.Item.BatchAccount)))
            {
                throw new ArgumentException("Batch account name is missing.", nameof(configuration));
            }

            if (accountsAndSubnetsInOrder.Any(account => string.IsNullOrWhiteSpace(account.Item.SubnetId)))
            {
                throw new ArgumentException("Subnet Id is missing.", nameof(configuration));
            }

            var groupedAccountsAndSubnets = accountsAndSubnetsInOrder
                .GroupBy(s => s.Item.BatchAccount, StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (groupedAccountsAndSubnets.Any(group => group.Count() != 1))
            {
                throw new ArgumentException("Duplicate batch account names provided.", nameof(configuration));
            }

            if (groupedAccountsAndSubnets.Count != accountsAndSubnetsInOrder.Count)
            {
                throw new ArgumentException("Requested batch account not found.", nameof(configuration));
            }

            var accountsAndSubnetsLookup = groupedAccountsAndSubnets
                .ToLookup(g => g.Key, g => g.Single(), StringComparer.OrdinalIgnoreCase);

            try
            {
                return subscription.GetBatchAccountsAsync(cancellationToken: cancellationToken)
                    .Where(resource => accountsAndSubnetsLookup.Contains(resource.Id.Name))
                    .OrderBy(resource => accountsAndSubnetsLookup[resource.Id.Name].Single().Index)
                    .SelectAwaitWithCancellation(async (resource, token) => new ItemWithName<Response<BatchAccountResource>>(resource.Id.Name, await resource.GetAsync(token)))
                    .Select((resource, index) => new BatchAccountInfo(resource.Item.Value?.Data ?? throw new InvalidOperationException($"Batch account {resource.Name} not retrieved."), accountsAndSubnetsLookup[resource.Name].Single().Item.SubnetId, index, credential, cancellationToken));
            }
            catch (InvalidOperationException exception)
            {
                throw new ArgumentException("Batch accounts with duplicate name matching a requested name was found in the subscription.", nameof(configuration), exception);
            }
        }

        private async Task<int> RunAsync(Configuration configuration)
        {
            ArgumentException.ThrowIfNullOrEmpty(configuration.OutputFilePath);
            ArgumentException.ThrowIfNullOrEmpty(configuration.SubscriptionId);
            ArgumentNullException.ThrowIfNull(configuration.BatchAccounts);

            Console.CancelKeyPress += CancelKeyPress;

            Console.WriteLine("Starting...");
            TokenCredential tokenCredential = new DefaultAzureCredential();
            var client = new ArmClient(tokenCredential);
            var appCache = new MemoryCache(new MemoryCacheOptions());
            var cacheAndRetryHandler = new CachingRetryHandler(appCache, Options.Create(new RetryPolicyOptions()));
            var priceApiClient = new PriceApiClient(cacheAndRetryHandler, new NullLogger<PriceApiClient>());

            var subscription = client.GetSubscriptionResource(new ResourceIdentifier($"/subscriptions/{configuration.SubscriptionId}"));
            var batchAccounts = GetBatchAccountsAsync(subscription, tokenCredential, configuration, cancellationTokenSource.Token);

            var metadataGetters = new List<Func<Task>>
            {
                async () =>
                {
                    Console.WriteLine("Getting pricing data...");
                    var now = DateTime.UtcNow + TimeSpan.FromSeconds(60);
                    await foreach (var price in priceApiClient.GetAllPricingInformationForNonWindowsAndNonSpotVmsAsync(AzureLocation.WestEurope, cancellationTokenSource.Token)
                        .Where(p => p.effectiveStartDate < now)
                        .WithCancellation(cancellationTokenSource.Token))
                    {
                        switch (price.meterName.Contains("Low Priority", StringComparison.OrdinalIgnoreCase))
                        {
                            case true: // Low Priority
                                AddUpdatePrice(lowPrPriceForVm, price);
                                break;

                            case false: // Dedicated
                                AddUpdatePrice(priceForVm, price);
                                break;
                        }

                        static void AddUpdatePrice(Dictionary<string, PricingItem> prices, PricingItem price)
                        {
                            if (prices.TryGetValue(price.armSkuName, out var existing))
                            {
                                if (price.effectiveStartDate > existing.effectiveStartDate)
                                {
                                    prices[price.armSkuName] = price;
                                }
                            }
                            else
                            {
                                prices.Add(price.armSkuName, price);
                            }
                        }
                    }
                },

                async () =>
                {
                    Console.WriteLine("Getting SKU information from each region in the subscription...");
                    await Parallel.ForEachAsync(subscription.GetLocationsAsync(cancellationToken: cancellationTokenSource.Token)
                            .Where(x => x.Metadata.RegionType == RegionType.Physical),
                        cancellationTokenSource.Token,
                        async (region, token) =>
                        {
                            try
                            {
                                List<ComputeResourceSku>? skus = null;
                                var count = 0;

                                await foreach (var vm in subscription.GetBatchSupportedVirtualMachineSkusAsync(region, cancellationToken: token).Select(s => s.Name).WithCancellation(token))
                                {
                                    ++count;
                                    _ = regionsForVm.AddOrUpdate(vm, _ => ImmutableHashSet<string>.Empty.Add(region.Name), (_, value) => value.Add(region.Name));

                                    skus ??= await subscription.GetComputeResourceSkusAsync($"location eq '{region.Name}'", cancellationToken: token).ToListAsync(token);
                                    _ = skuForVm.GetOrAdd(vm, vm =>
                                        skus.Single(sku => sku.Name.Equals(vm, StringComparison.OrdinalIgnoreCase)));
                                }

                                Console.WriteLine($"{region.Name} supportedSkuCount:{count}");
                            }
                            catch (RequestFailedException e)
                            {
                                Console.ForegroundColor = ConsoleColor.Yellow;
                                Console.WriteLine($"No skus supported in {region.Name}. {e.ErrorCode}");
                                Console.ResetColor();
                            }
                        });
                }
            };

            await Task.WhenAll(metadataGetters.Select(async task => await task()));

            var batchSupportedVmSet = regionsForVm.Keys
                .SelectMany(GetVirtualMachineInformations)
                .GroupBy(i => i.VmSize, StringComparer.OrdinalIgnoreCase)
                .Select(VmSku.Create)
                .ToList();

            Console.WriteLine($"Superset supportedSkuCount:{batchSupportedVmSet.Count}");

            Console.WriteLine("Verifying SKUs in Azure Batch...");
            var (verified, notVerified) = await AzureBatchSkuValidator.ValidateSkus(batchSupportedVmSet, batchAccounts, batchSupportedVmSet.Select(sku => sku.Sku).ToList(), cancellationTokenSource.Token);

            notVerified = notVerified.ToList();

            if (notVerified.Any())
            {
                Console.WriteLine(new string(Enumerable.Repeat(' ', 80).ToArray()));
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("Due to either availability, capacity, quota, or other constraints, the following SKUs were not validated and will not be included:");
                notVerified.Select(vmsize => (vmsize.Name, vmsize.Sku.LowPriority))
                    .OrderBy(sku => sku.Name, StringComparer.OrdinalIgnoreCase)
                    .ForEach(sku => Console.WriteLine($"    '{sku.Name}'{DedicatedMarker(sku.LowPriority)} with availability in regions '{string.Join("', '", regionsForVm[sku.Name].OrderBy(region => region, StringComparer.OrdinalIgnoreCase))}'."));
                Console.ResetColor();

                static string DedicatedMarker(bool hasLowPriority) => hasLowPriority ? string.Empty : " (dedicatedOnly)";
            }

            var batchVmInfo = verified
                .OrderBy(x => x!.VmSize)
                .ThenBy(x => x!.LowPriority)
                .ToList();

            Console.WriteLine($"SupportedSkuCount:{batchVmInfo.Select(vm => vm.VmSize).Distinct(StringComparer.OrdinalIgnoreCase).Count()}");
            Console.WriteLine($"Writing {batchVmInfo.Count} SKU price records");

            var jsonOptions = new JsonSerializerOptions(JsonSerializerDefaults.General)
            {
                WriteIndented = true
            };

            var data = JsonSerializer.Serialize(batchVmInfo, options: jsonOptions);
            await File.WriteAllTextAsync(configuration.OutputFilePath!, data, cancellationTokenSource.Token);
            return 0;
        }

        private IEnumerable<VirtualMachineInformation> GetVirtualMachineInformations(string name)
        {
            if (name is null)
            {
                yield break;
            }

            var sku = skuForVm[name] ?? throw new Exception($"Sku info is null for VM {name}");

            var generations = AttemptParseString(name)?.Split(",").ToList() ?? new();
            var vCpusAvailable = AttemptParseInt32("vCPUsAvailable");
            var encryptionAtHostSupported = AttemptParseBoolean("EncryptionAtHostSupported");
            var lowPriorityCapable = AttemptParseBoolean("LowPriorityCapable") ?? false;
            var maxDataDiskCount = AttemptParseInt32("MaxDataDiskCount");
            var maxResourceVolumeMB = AttemptParseInt32("MaxResourceVolumeMB");
            var memoryGB = AttemptParseDouble("MemoryGB");

            yield return new()
            {
                MaxDataDiskCount = maxDataDiskCount,
                MemoryInGiB = memoryGB,
                VCpusAvailable = vCpusAvailable,
                ResourceDiskSizeInGiB = ConvertMiBToGiB(maxResourceVolumeMB),
                VmSize = sku.Name,
                VmFamily = sku.Family,
                LowPriority = false,
                HyperVGenerations = generations,
                RegionsAvailable = regionsForVm[name].Order().ToList(),
                EncryptionAtHostSupported = encryptionAtHostSupported,
                PricePerHour = priceForVm.TryGetValue(name, out var priceItem) ? (decimal)priceItem.retailPrice : null,
            };

            if (lowPriorityCapable)
            {
                yield return new()
                {
                    MaxDataDiskCount = maxDataDiskCount,
                    MemoryInGiB = memoryGB,
                    VCpusAvailable = vCpusAvailable,
                    ResourceDiskSizeInGiB = ConvertMiBToGiB(maxResourceVolumeMB),
                    VmSize = sku.Name,
                    VmFamily = sku.Family,
                    LowPriority = true,
                    HyperVGenerations = generations,
                    RegionsAvailable = regionsForVm[name].Order().ToList(),
                    EncryptionAtHostSupported = encryptionAtHostSupported,
                    PricePerHour = lowPrPriceForVm.TryGetValue(name, out var lowPrPriceItem) ? (decimal)lowPrPriceItem.retailPrice : null,
                };
            }

            string? AttemptParseString(string name)
                => sku!.Capabilities.SingleOrDefault(x => x.Name.Equals(name, StringComparison.OrdinalIgnoreCase))?.Value;

            bool? AttemptParseBoolean(string name)
                => bool.TryParse(AttemptParseString(name), out var value) ? value : null;

            int? AttemptParseInt32(string name)
                => int.TryParse(AttemptParseString(name), System.Globalization.NumberFormatInfo.InvariantInfo, out var value) ? value : null;

            double? AttemptParseDouble(string name)
                => double.TryParse(AttemptParseString(name), System.Globalization.NumberFormatInfo.InvariantInfo, out var value) ? value : null;
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
            private Microsoft.Azure.Batch.BatchClient ClientFactory(TokenCredential credential, CancellationToken cancellationToken)
            {
                var result = Microsoft.Azure.Batch.BatchClient.Open(
                new Microsoft.Azure.Batch.Auth.BatchTokenCredentials(
                    new UriBuilder(Uri.UriSchemeHttps, data.AccountEndpoint).Uri.AbsoluteUri,
                    async () => await BatchTokenProvider(credential, cancellationToken)));

                // replace default retry policy
                //result.CustomBehaviors.OfType<Microsoft.Azure.Batch.RetryPolicyProvider>().Single().Policy = new Microsoft.Azure.Batch.Common.ExponentialRetry(TimeSpan.FromSeconds(1), 9);
                return result;
            }

            private async Task<string> BatchTokenProvider(TokenCredential tokenCredential, CancellationToken cancellationToken)
            {
                if (accessToken?.ExpiresOn > DateTimeOffset.UtcNow.Subtract(TimeSpan.FromSeconds(60)))
                {
                    return accessToken.Value.Token;
                }

                accessToken = await tokenCredential.GetTokenAsync(
                    new(new[] { new UriBuilder("https://batch.core.windows.net/") { Path = ".default" }.Uri.AbsoluteUri }, Guid.NewGuid().ToString("D")),
                    cancellationToken);
                return accessToken.Value.Token;
            }

            private readonly Lazy<Microsoft.Azure.Batch.BatchClient> batchClient;
            private readonly BatchAccountData data;
            private AccessToken? accessToken;

            public BatchAccountInfo(BatchAccountData data, string subnetId, int index, TokenCredential credential, CancellationToken cancellationToken)
            {
                SubnetId = subnetId;
                Index = index;
                this.data = data;
                batchClient = new Lazy<Microsoft.Azure.Batch.BatchClient>(() => ClientFactory(credential, cancellationToken));
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
