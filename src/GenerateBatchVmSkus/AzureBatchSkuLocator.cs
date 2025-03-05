// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.CommandLine.Rendering;
using System.Globalization;
using System.Reflection;
using System.Text.Json;
using Azure;
using Azure.Core;
using Azure.ResourceManager;
using Azure.ResourceManager.Batch;
using Azure.ResourceManager.Compute;
using Azure.ResourceManager.Compute.Models;
using CommonUtilities;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Tes.ApiClients;
using Tes.ApiClients.Models.Pricing;
using Tes.Models;
using static GenerateBatchVmSkus.AzureBatchSkuValidator;
using static GenerateBatchVmSkus.Program;

namespace GenerateBatchVmSkus
{
    /*
     * TODO considerations:
     *   Currently, we consider the Azure APIs to return the same values for each region where any given SKU exists, but that is not the case. Consider implementing a strategy where we, on a case-by-case basis, determine whether we want the "max" or the "min" of all the regions sampled for any given property.
     */

    internal class AzureBatchSkuLocator(Configuration configuration, IOptions<CommonUtilities.Options.RetryPolicyOptions> retryPolicyOptions, ArmClient client, Func<BatchAccountData, string, int, CancellationToken, BatchAccountInfo> batchAccountInfoFactory, IConsole console) : ICommandHandler
    {
        private static readonly ConcurrentDictionary<string, ImmutableHashSet<string>> regionsForVm = new(StringComparer.OrdinalIgnoreCase);
        private static readonly ConcurrentDictionary<string, ComputeResourceSku> skuForVm = new(StringComparer.OrdinalIgnoreCase);
        private static readonly Dictionary<string, PricingItem> priceForVm = new(StringComparer.OrdinalIgnoreCase);
        private static readonly Dictionary<string, PricingItem> lowPrPriceForVm = new(StringComparer.OrdinalIgnoreCase);
        private static IEnumerable<StorageDiskPriceInformation> diskPrices = [];

        public int Invoke(InvocationContext context) => throw new NotSupportedException();

        public async Task<int> InvokeAsync(InvocationContext context)
        {
            ArgumentException.ThrowIfNullOrEmpty(configuration.VmOutputFilePath);
            ArgumentException.ThrowIfNullOrEmpty(configuration.DiskOutputFilePath);
            ArgumentException.ThrowIfNullOrEmpty(configuration.SubscriptionId);
            ArgumentNullException.ThrowIfNull(configuration.BatchAccounts);

            ConsoleHelper.Console = console;
            return await RunAsync(
                context.ParseResult.CommandResult.GetValueForOption(Program.NoValidateWhitelistFile),
                context.ParseResult.CommandResult.GetValueForOption(Program.CloudName)!,
                context.GetCancellationToken());
        }

        private static double? ConvertMiBToGiB(int? value) => value.HasValue ? Math.Round(value.Value / 1024.0, 2) : null;

        private record struct ItemWithIndex<T>(T Item, int Index);
        private record struct ItemWithName<T>(string Name, T Item);

        private IAsyncEnumerable<BatchAccountInfo> GetBatchAccountsAsync(Azure.ResourceManager.Resources.SubscriptionResource subscription, Configuration configuration, CancellationToken cancellationToken)
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
                    .Select((resource, index) => batchAccountInfoFactory(resource.Item.Value?.Data ?? throw new InvalidOperationException($"Batch account {resource.Name} not retrieved."), accountsAndSubnetsLookup[resource.Name].Single().Item.SubnetId, index, cancellationToken));
            }
            catch (InvalidOperationException exception)
            {
                throw new ArgumentException("Batch accounts with duplicate name matching a requested name was found in the subscription.", nameof(configuration), exception);
            }
        }

        private static void AddUpdatePrice(Dictionary<string, PricingItem> prices, PricingItem price, string key)
        {
            if (prices.TryGetValue(key, out var existing))
            {
                if (price.effectiveStartDate > existing.effectiveStartDate)
                {
                    prices[key] = price;
                }
            }
            else
            {
                prices.Add(key, price);
            }
        }

        internal async Task<int> RunAsync(FileInfo? whitelistFile, string cloudName, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrEmpty(cloudName);

            ConsoleHelper.WriteLine("Starting...");
            var appCache = new MemoryCache(new MemoryCacheOptions());
            var cacheAndRetryHandler = new CachingRetryPolicyBuilder(appCache, retryPolicyOptions);
            var priceApiClient = new PriceApiClient(cacheAndRetryHandler, new NullLogger<PriceApiClient>());

            var subscription = client.GetSubscriptionResource(new ResourceIdentifier($"/subscriptions/{configuration.SubscriptionId}"));

            var startTime = DateTimeOffset.UtcNow;
            var metadataGetters = new List<Func<Task>>
            {
                async () =>
                {
                    ConsoleHelper.WriteLine("Getting pricing data...");
                    var now = DateTime.UtcNow + TimeSpan.FromSeconds(60);
                    await foreach (var price in priceApiClient.GetAllPricingInformationForNonWindowsAndNonSpotVmsAsync(AzureLocation.WestEurope, cancellationToken)
                        .Where(p => p.effectiveStartDate < now)
                        .WithCancellation(cancellationToken))
                    {
                        switch (price.meterName.Contains("Low Priority", StringComparison.OrdinalIgnoreCase))
                        {
                            case true: // Low Priority
                                AddUpdatePrice(lowPrPriceForVm, price, price.armSkuName);
                                break;

                            case false: // Dedicated
                                AddUpdatePrice(priceForVm, price, price.armSkuName);
                                break;
                        }
                    }
                },

                async () =>
                {
                    Console.WriteLine("Getting Storage pricing data...");
                    var now = DateTime.UtcNow + TimeSpan.FromSeconds(60);
                    Dictionary<string, PricingItem> prices = [];
                    await foreach (var price in priceApiClient.GetAllPricingInformationForStandardStorageLRSDisksAsync(AzureLocation.WestEurope, cancellationToken)
                        .Where(p => p.effectiveStartDate < now)
                        .Where(p => "1/Month".Equals(p.unitOfMeasure, StringComparison.OrdinalIgnoreCase))
                        .Where(p => StorageDiskPriceInformation.StandardLrsSsdCapacityInGiB.ContainsKey(p.meterName))
                        .WithCancellation(cancellationToken))
                    {
                        price.unitPrice = StorageDiskPriceInformation.ConvertPricePerMonthToPricePerHour(price.unitPrice);
                        price.tierMinimumUnits = StorageDiskPriceInformation.ConvertPricePerMonthToPricePerHour(price.tierMinimumUnits);
                        price.unitOfMeasure = "1 Hour";

                        AddUpdatePrice(prices, price, price.meterName);
                    }

                    diskPrices = [.. diskPrices
                        .Concat(prices.Values.Select(item => new StorageDiskPriceInformation(item.meterName, StorageDiskPriceInformation.StandardLrsSsdCapacityInGiB[item.meterName], Convert.ToDecimal(item.unitPrice))))
                        .OrderBy(item => item.CapacityInGiB)];
                },

                async () =>
                {
                    ConsoleHelper.WriteLine("Getting SKU information from each region in the subscription...");
                    await Parallel.ForEachAsync(subscription.GetLocationsAsync(includeExtendedLocations: true, cancellationToken: cancellationToken),
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

                                    skusInSkus ??= [.. skus.Select(sku => sku.Name)];
                                    if (!skusInSkus.Contains(vm, StringComparer.OrdinalIgnoreCase))
                                    {
                                        ConsoleHelper.WriteLine(ForegroundColorSpan.LightYellow(), $"Warning: '{vm}' in the list of batch supported SKUs is not found in the compute resources of region {region.DisplayName}.");
                                        continue;
                                    }

                                    _ = skuForVm.GetOrAdd(vm, vm =>
                                        skus.Single(sku => sku.Name.Equals(vm, StringComparison.OrdinalIgnoreCase)));
                                }

                                ConsoleHelper.WriteLine($"{region.DisplayName} supportedSkuCount:{count}");
                            }
                            catch (RequestFailedException e)
                            {
                                ConsoleHelper.WriteLine(ForegroundColorSpan.LightYellow(), $"No skus supported in {region.DisplayName}. {e.ErrorCode}");
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

            ConsoleHelper.WriteLine($"Superset supportedSkuCount:{batchSupportedVmSet.Count}");
            ConsoleHelper.WriteLine("Retrieving data from Azure", ForegroundColorSpan.Green(), $"Completed in {DateTimeOffset.UtcNow - startTime:g}.");

            Func<Task<ValidationResults>> batchValidator = new(async () =>
            {
                ConsoleHelper.WriteLine("Validating SKUs in Azure Batch...");
                return await ValidateSkus(
                    batchSupportedVmSet,
                    GetBatchAccountsAsync(subscription, configuration, cancellationToken),
                    skuForVm
                        .Select(sku => (sku.Key, Info: new BatchSkuInfo(
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
                string oneIndent = new([.. Enumerable.Repeat(' ', indent)]);
                string twoIndent = new([.. Enumerable.Repeat(' ', indent * 2)]);
                string threeIndent = new([.. Enumerable.Repeat(' ', indent * 3)]);
                var tableWidth = console.IsOutputRedirected ? 0 : Console.WindowWidth - threeIndent.Length;
                ConsoleHelper.WriteLine(ForegroundColorSpan.LightYellow(), $"Due to either availability, capacity, quota, or other constraints, the following {notVerified.Count()} SKUs were not validated and will not be included. Consider adding quota or additional regions as needed:");
                notVerified.Select(vmsize => (vmsize.Sku.VmSize, vmsize.Sku.VmFamily, vmsize.Sku.LowPriority, vmsize.Sku.RegionsAvailable))
                    .GroupBy(sku => sku.VmFamily, sku => (sku.VmSize, sku.LowPriority, sku.RegionsAvailable), StringComparer.OrdinalIgnoreCase)
                    .OrderBy(family => family.Key, StringComparer.OrdinalIgnoreCase)
                    .ForEach(family =>
                    {
                        ConsoleHelper.WriteLine($"{oneIndent}Family: '{family.Key}'");
                        family.OrderBy(sku => sku.VmSize)
                            .ThenBy(sku => sku.LowPriority)
                            .ForEach(sku => ConsoleHelper.WriteLine(
                                $"{twoIndent}'{sku.VmSize}'{DedicatedMarker(sku.LowPriority)} regions available:{Environment.NewLine}{threeIndent}{string.Join($"{Environment.NewLine}{threeIndent}", MakeTable(tableWidth, sku.RegionsAvailable.OrderBy(region => region, StringComparer.OrdinalIgnoreCase), " ").Select(s => s.TrimEnd()))}"));
                    });
                ConsoleHelper.WriteLine(string.Empty);

                static string DedicatedMarker(bool hasLowPriority) => hasLowPriority ? string.Empty : " (dedicatedOnly)";

                // returns rows with equal-width columns
                static IEnumerable<string> MakeTable(int tableWidth, IEnumerable<string> items, string separator)
                {
                    if (tableWidth <= 0)
                    {
                        return items;
                    }

                    items = items.ToList(); // to safely enumerate twice
                    var columnWidth = items.Max(item => item.Length);
                    var columns = (tableWidth + separator.Length) / (columnWidth + separator.Length);
                    return items.ConvertGroup(columns, (item, _) => item + new string([.. Enumerable.Repeat(' ', columnWidth - item.Length)]), row => string.Join(separator, row));
                }
            }

            var batchVmInfo = verified
                .OrderBy(x => x!.VmSize)
                .ThenBy(x => x!.LowPriority)
                .ToList();

            ConsoleHelper.WriteLine($"SupportedSkuCount:{batchVmInfo.Select(vm => vm.VmSize).Distinct(StringComparer.OrdinalIgnoreCase).Count()}");
            ConsoleHelper.WriteLine($"Writing {batchVmInfo.Count} SKU price records");

            await Task.WhenAll(WriteOutput((configuration.VmOutputFilePath!, batchVmInfo)), WriteOutput((configuration.DiskOutputFilePath!, diskPrices)));
            ConsoleHelper.WriteLine(Assembly.GetEntryAssembly()!.GetName().Name!, ForegroundColorSpan.Green(), $"Completed in {DateTimeOffset.UtcNow - startTime:g}.");
            return 0;

            Task WriteOutput<T>((string Path, T Value) output)
            {
                var data = JsonSerializer.Serialize(output.Value, options: jsonOptions);
                return File.WriteAllTextAsync(output.Path, data, cancellationToken);
            }
        }

        private static IEnumerable<VirtualMachineInformation> GetVirtualMachineInformations(string name)
        {
            if (name is null)
            {
                yield break;
            }

            var sku = skuForVm[name] ?? throw new Exception($"Sku info is null for VM {name}");

            //if (name.Contains("_L"))
            //{
            //    System.Diagnostics.Debugger.Break();
            //}

            var generations = AttemptParseString("HyperVGenerations")?.Split(",").ToList() ?? [];
            var vCpusAvailable = AttemptParseInt32("vCPUsAvailable");
            var encryptionAtHostSupported = AttemptParseBoolean("EncryptionAtHostSupported");
            var lowPriorityCapable = AttemptParseBoolean("LowPriorityCapable") ?? false;
            var maxDataDiskCount = AttemptParseInt32("MaxDataDiskCount");
            var maxResourceVolumeMB = AttemptParseInt32("MaxResourceVolumeMB");
            var memoryGB = AttemptParseDouble("MemoryGB");
            var mvmeMB = AttemptParseInt32("NvmeDiskSizeInMiB");
            var gpus = AttemptParseInt32("GPUs");

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
                RegionsAvailable = [.. regionsForVm[name].Order()],
                EncryptionAtHostSupported = encryptionAtHostSupported,
                NvmeDiskSizeInGiB = ConvertMiBToGiB(mvmeMB),
                GpusAvailable = gpus,
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
                    RegionsAvailable = [.. regionsForVm[name].Order()],
                    EncryptionAtHostSupported = encryptionAtHostSupported,
                    NvmeDiskSizeInGiB = ConvertMiBToGiB(mvmeMB),
                    GpusAvailable = gpus,
                    PricePerHour = lowPrPriceForVm.TryGetValue(name, out var lowPrPriceItem) ? (decimal)lowPrPriceItem.retailPrice : null,
                };
            }

            string? AttemptParseString(string name)
                => sku!.Capabilities.SingleOrDefault(x => x.Name.Equals(name, StringComparison.OrdinalIgnoreCase))?.Value;

            bool? AttemptParseBoolean(string name)
                => bool.TryParse(AttemptParseString(name), out var value) ? value : null;

            int? AttemptParseInt32(string name)
                => int.TryParse(AttemptParseString(name), NumberFormatInfo.InvariantInfo, out var value) ? value : null;

            double? AttemptParseDouble(string name)
                => double.TryParse(AttemptParseString(name), NumberFormatInfo.InvariantInfo, out var value) ? value : null;
        }
    }
}
