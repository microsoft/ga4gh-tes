// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.CommandLine.Invocation;
using System.Globalization;
using System.Reflection;
using System.Text.Json;
using Azure;
using Azure.Core;
using Azure.Identity;
using Azure.ResourceManager;
using Azure.ResourceManager.Batch;
using Azure.ResourceManager.Compute;
using Azure.ResourceManager.Compute.Models;
using Azure.ResourceManager.Resources.Models;
using CommonUtilities.Options;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Tes.ApiClients;
using Tes.ApiClients.Models.Pricing;
using Tes.Models;
using static GenerateBatchVmSkus.AzureBatchSkuValidator;

namespace GenerateBatchVmSkus
{
    internal class AzureBatchSkuLocator(Configuration configuration) : ICommandHandler
    {
        private static readonly ConcurrentDictionary<string, ImmutableHashSet<string>> regionsForVm = new(StringComparer.OrdinalIgnoreCase);
        private static readonly ConcurrentDictionary<string, ComputeResourceSku> skuForVm = new(StringComparer.OrdinalIgnoreCase);
        private static readonly Dictionary<string, PricingItem> priceForVm = new(StringComparer.OrdinalIgnoreCase);
        private static readonly Dictionary<string, PricingItem> lowPrPriceForVm = new(StringComparer.OrdinalIgnoreCase);

        public int Invoke(InvocationContext context) => InvokeAsync(context).Result;

        public async Task<int> InvokeAsync(InvocationContext context)
        {
            return await RunAsync(
                context.ParseResult.CommandResult.GetValueForOption(Program.NoValidateWhitelistFile),
                context.ParseResult.CommandResult.GetValueForOption(Program.CloudName)!,
                context.GetCancellationToken());
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

        internal async Task<int> RunAsync(FileInfo? whitelistFile, string cloudName, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrEmpty(configuration.OutputFilePath);
            ArgumentException.ThrowIfNullOrEmpty(configuration.SubscriptionId);
            ArgumentNullException.ThrowIfNull(configuration.BatchAccounts);

            Console.WriteLine("Starting...");
            TokenCredential tokenCredential = new DefaultAzureCredential(new DefaultAzureCredentialOptions { AuthorityHost = cloudName switch
            {
                nameof(AzureAuthorityHosts.AzurePublicCloud) => AzureAuthorityHosts.AzurePublicCloud,
                nameof(AzureAuthorityHosts.AzureGovernment) => AzureAuthorityHosts.AzureGovernment,
                nameof(AzureAuthorityHosts.AzureChina) => AzureAuthorityHosts.AzureChina,
                _ => throw new ArgumentOutOfRangeException(nameof(cloudName)),
            }});
            RetryPolicyOptions retryPolicyOptions = new();
            var clientOptions = new ArmClientOptions();
            clientOptions.Environment = cloudName switch
            {
                nameof(ArmEnvironment.AzurePublicCloud) => ArmEnvironment.AzurePublicCloud,
                nameof(ArmEnvironment.AzureGovernment) => ArmEnvironment.AzureGovernment,
                nameof(ArmEnvironment.AzureChina) => ArmEnvironment.AzureChina,
                _ => throw new ArgumentOutOfRangeException(nameof(cloudName)),
            };
            clientOptions.Retry.Mode = RetryMode.Exponential;
            clientOptions.Retry.MaxRetries = int.Max(clientOptions.Retry.MaxRetries, retryPolicyOptions.MaxRetryCount);
            var client = new ArmClient(tokenCredential, default, clientOptions);
            var appCache = new MemoryCache(new MemoryCacheOptions());
            var cacheAndRetryHandler = new CachingRetryPolicyBuilder(appCache, Options.Create(retryPolicyOptions));
            var priceApiClient = new PriceApiClient(cacheAndRetryHandler, new NullLogger<PriceApiClient>());

            var subscription = client.GetSubscriptionResource(new ResourceIdentifier($"/subscriptions/{configuration.SubscriptionId}"));
            var batchAccounts = GetBatchAccountsAsync(subscription, tokenCredential, configuration, cancellationToken);

            var startTime = DateTimeOffset.UtcNow;
            var metadataGetters = new List<Func<Task>>
            {
                async () =>
                {
                    Console.WriteLine("Getting pricing data...");
                    var now = DateTime.UtcNow + TimeSpan.FromSeconds(60);
                    await foreach (var price in priceApiClient.GetAllPricingInformationForNonWindowsAndNonSpotVmsAsync(AzureLocation.WestEurope, cancellationToken)
                        .Where(p => p.effectiveStartDate < now)
                        .WithCancellation(cancellationToken))
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
                    await Parallel.ForEachAsync(subscription.GetLocationsAsync(cancellationToken: cancellationToken)
                            .Where(x => x.Metadata.RegionType == RegionType.Physical),
                        cancellationToken,
                        async (region, token) =>
                        {
                            try
                            {
                                List<ComputeResourceSku>? skus = null;
                                List<string>? skusInSkus = null;
                                var count = 0;

                                await foreach (var vm in subscription.GetBatchSupportedVirtualMachineSkusAsync(region, cancellationToken: token).Select(s => s.Name).WithCancellation(token))
                                {
                                    ++count;
                                    _ = regionsForVm.AddOrUpdate(vm, _ => ImmutableHashSet<string>.Empty.Add(region.Name), (_, value) => value.Add(region.Name));

                                    skus ??= await subscription.GetComputeResourceSkusAsync($"location eq '{region.Name}'", cancellationToken: token).ToListAsync(token);

                                    skusInSkus ??= skus.Select(sku => sku.Name).ToList();
                                    if (!skusInSkus.Contains(vm))
                                    {
                                        System.Diagnostics.Debugger.Break();
                                    }

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
            AzureBatchSkuValidator.ConsoleWriteLine("Retrieving data from Azure", ConsoleColor.Green, $"Completed in {DateTimeOffset.UtcNow - startTime:g}.");

            Func<Task<ValidationResults>> batchValidator = new(async () =>
            {
                Console.WriteLine("Validating SKUs in Azure Batch...");
                return await AzureBatchSkuValidator.ValidateSkus(
                    batchSupportedVmSet,
                    batchAccounts,
                    skuForVm
                        .Select(sku => (sku.Key, Info: new AzureBatchSkuValidator.BatchSkuInfo(
                            sku.Value.Family,
                            int.TryParse(sku.Value.Capabilities.SingleOrDefault(x => x.Name.Equals("vCPUsAvailable", StringComparison.OrdinalIgnoreCase))?.Value, NumberFormatInfo.InvariantInfo, out var vCpus) ? vCpus : null)))
                        .ToDictionary(sku => sku.Key, sku => sku.Info, StringComparer.OrdinalIgnoreCase),
                    cancellationToken);
            });

            var jsonOptions = new JsonSerializerOptions(JsonSerializerDefaults.General)
            {
                WriteIndented = true,
                UnmappedMemberHandling = System.Text.Json.Serialization.JsonUnmappedMemberHandling.Skip
            };

            Func<Task<ValidationResults>> whiteListValidator = new(async () =>
            {
                using var stream = whitelistFile!.OpenRead();
                var whiteList = (await JsonSerializer.DeserializeAsync<VirtualMachineInformation[]>(stream, jsonOptions, cancellationToken)) ?? [];
                var supported = batchSupportedVmSet.Select(s => s.Name).Intersect(whiteList.Select(s => s.VmSize).Distinct(), StringComparer.OrdinalIgnoreCase).ToList();
                var not = batchSupportedVmSet.Select(s => s.Name).Except(supported, StringComparer.OrdinalIgnoreCase).ToList();
                return new(supported.SelectMany(GetVirtualMachineInformations), batchSupportedVmSet.Where(s => not.Contains(s.Name, StringComparer.OrdinalIgnoreCase)));
            });

            var validationResult = (whitelistFile is null) switch
            {
                true => batchValidator(),
                false => whiteListValidator(),
            };

            var (verified, notVerified) = await validationResult;

            notVerified = notVerified.ToList();

            if (notVerified.Any())
            {
                var indent = 4;
                string oneIndent = new(Enumerable.Repeat(' ', indent).ToArray());
                string twoIndent = new(Enumerable.Repeat(' ', indent * 2).ToArray());
                string threeIndent = new(Enumerable.Repeat(' ', indent * 3).ToArray());
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"Due to either availability, capacity, quota, or other constraints, the following {notVerified.Count()} SKUs were not validated and will not be included. Consider adding quota or additional regions as needed:");
                Console.ResetColor();
                notVerified.Select(vmsize => (vmsize.Sku.VmSize, vmsize.Sku.VmFamily, vmsize.Sku.LowPriority, vmsize.Sku.RegionsAvailable))
                    .GroupBy(sku => sku.VmFamily, sku => (sku.VmSize, sku.LowPriority, sku.RegionsAvailable), StringComparer.OrdinalIgnoreCase)
                    .OrderBy(family => family.Key, StringComparer.OrdinalIgnoreCase)
                    .ForEach(family =>
                    {
                        Console.WriteLine($"{oneIndent}Family: '{family.Key}'");
                        family.OrderBy(sku => sku.VmSize)
                            .ThenBy(sku => sku.LowPriority)
                            .ForEach(sku => Console.WriteLine($"{twoIndent}'{sku.VmSize}'{DedicatedMarker(sku.LowPriority)} regions available:{Environment.NewLine}{threeIndent}'{string.Join($"'{Environment.NewLine}{threeIndent}'", sku.RegionsAvailable.OrderBy(region => region, StringComparer.OrdinalIgnoreCase))}'."));
                    });
                Console.WriteLine();

                static string DedicatedMarker(bool hasLowPriority) => hasLowPriority ? string.Empty : " (dedicatedOnly)";
            }

            var batchVmInfo = verified
                .OrderBy(x => x!.VmSize)
                .ThenBy(x => x!.LowPriority)
                .ToList();

            Console.WriteLine($"SupportedSkuCount:{batchVmInfo.Select(vm => vm.VmSize).Distinct(StringComparer.OrdinalIgnoreCase).Count()}");
            Console.WriteLine($"Writing {batchVmInfo.Count} SKU price records");

            var data = JsonSerializer.Serialize(batchVmInfo, options: jsonOptions);
            await File.WriteAllTextAsync(configuration.OutputFilePath!, data, cancellationToken);
            AzureBatchSkuValidator.ConsoleWriteLine(Assembly.GetEntryAssembly()!.GetName().Name!, ConsoleColor.Green, $"Completed in {DateTimeOffset.UtcNow - startTime:g}.");
            return 0;
        }

        private static IEnumerable<VirtualMachineInformation> GetVirtualMachineInformations(string name)
        {
            if (name is null)
            {
                yield break;
            }

            var sku = skuForVm[name] ?? throw new Exception($"Sku info is null for VM {name}");

            var generations = AttemptParseString("HyperVGenerations")?.Split(",").ToList() ?? new();
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

                result.CustomBehaviors.OfType<Microsoft.Azure.Batch.RetryPolicyProvider>().Single().Policy = new Microsoft.Azure.Batch.Common.ExponentialRetry(TimeSpan.FromSeconds(1), 11, TimeSpan.FromMinutes(1));
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
