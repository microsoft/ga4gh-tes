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

namespace TesUtils
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
        private static Microsoft.Azure.Batch.ImageReference V1ImageReference
            => new("ubuntu-server-container", "microsoft-azure-batch", "20-04-lts", "latest");

        private static Microsoft.Azure.Batch.ImageReference V2ImageReference
            => new("ubuntu-hpc", "microsoft-dsvm", "2004", "latest");

        private static Microsoft.Azure.Batch.VirtualMachineConfiguration GetMachineConfiguration(bool useV2, bool encryptionAtHostSupported)
        {
            return new(useV2 ? V2ImageReference : V1ImageReference, "batch.node.ubuntu 20.04")
            {
                DiskEncryptionConfiguration = encryptionAtHostSupported ? new Microsoft.Azure.Batch.DiskEncryptionConfiguration(targets: new List<Microsoft.Azure.Batch.Common.DiskEncryptionTarget>() { Microsoft.Azure.Batch.Common.DiskEncryptionTarget.OsDisk, Microsoft.Azure.Batch.Common.DiskEncryptionTarget.TemporaryDisk }) : null
            };
        }

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

        private enum VerifyVMIResult
        {
            Use,
            Skip,
            NextRegion,
            Retry
        }

        private readonly object lockObj = new();
        private string linePrefix = string.Empty;

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

            var now = DateTime.UtcNow + TimeSpan.FromSeconds(60);

            Console.WriteLine("Getting pricing data...");
            await foreach (var price in priceApiClient.GetAllPricingInformationForNonWindowsAndNonSpotVmsAsync(AzureLocation.WestEurope, cancellationTokenSource.Token)
                .Where(p => p.effectiveStartDate < now)
                .WithCancellation(cancellationTokenSource.Token))
            {
                if (price.meterName.Contains("Low Priority", StringComparison.OrdinalIgnoreCase))
                {
                    if (lowPrPriceForVm.TryGetValue(price.armSkuName, out var existing))
                    {
                        if (price.effectiveStartDate > existing.effectiveStartDate)
                        {
                            lowPrPriceForVm[price.armSkuName] = price;
                        }
                    }
                    else
                    {
                        lowPrPriceForVm.Add(price.armSkuName, price);
                    }
                }
                else
                {
                    if (priceForVm.TryGetValue(price.armSkuName, out var existing))
                    {
                        if (price.effectiveStartDate > existing.effectiveStartDate)
                        {
                            priceForVm[price.armSkuName] = price;
                        }
                    }
                    else
                    {
                        priceForVm.Add(price.armSkuName, price);
                    }
                }
            }

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

            var batchSupportedVmSet = regionsForVm.Keys
                .SelectMany(GetVirtualMachineInformations)
                .GroupBy(i => i.VmSize).ToList();
            Console.WriteLine($"Superset supportedSkuCount:{batchSupportedVmSet.Count}");

            Console.WriteLine("Verifying each SKU...");
            List<VirtualMachineInformation> vms = new();
            List<IGrouping<string, VirtualMachineInformation>> vmsForNextRegion = new();
            vmsForNextRegion.AddRange(batchSupportedVmSet);

            await foreach (var batchAccount in batchAccounts.WithCancellation(cancellationTokenSource.Token))
            {
                Dictionary<string, int> retryCounts = new(StringComparer.OrdinalIgnoreCase);
                using var disposable = batchAccount;
                var vmsToTest = vmsForNextRegion.ToList();
                vmsForNextRegion.Clear();

                Console.WriteLine(linePrefix + $"Verifying {vmsToTest.Count} SKUs with {batchAccount.Name}.");
                linePrefix = string.Empty;

                vmsToTest.ForEach(vm => retryCounts.Add(vm.Key, 0));

                var context = await GetTestQuotaContext(batchAccount, batchSupportedVmSet, cancellationTokenSource.Token);
                var loadedTests = vmsToTest.Where(vmSize => AddTestToQuotaIfQuotaPermits(vmSize, context)).ToList();

                var count = vmsToTest.Count;
                var current = loadedTests.Count;
                var index = 0;

                var StartLoadedTest = new Func<IGrouping<string, VirtualMachineInformation>, Task<(IGrouping<string, VirtualMachineInformation> vmSize, VerifyVMIResult result)>>(async vmSize =>
                {
                    _ = Interlocked.Increment(ref index);
                    return (vmSize, result: await TestVMSizeInBatchAsync(vmSize, batchAccount, cancellationTokenSource.Token));
                });

                for (var tests = loadedTests.Select(StartLoadedTest).ToList(); tests.Any(); tests.AddRange(loadedTests.Select(StartLoadedTest)))
                {
                    foreach (var loadedTest in loadedTests)
                    {
                        vmsToTest.Remove(loadedTest);
                    }

                    if (!Console.IsOutputRedirected)
                    {
                        lock (lockObj)
                        {
                            var line = $"[{batchAccount.Index}:{batchAccount.Name}] Verifying SKUs {Math.Max(0, index - current) / (double)count:P2} completed...";
                            var sb = new StringBuilder();
                            sb.Append(linePrefix);
                            sb.Append(line);
                            sb.Append('\r');
                            Console.Write(sb.ToString());
                            sb.Clear();

                            foreach (var @char in Enumerable.Repeat(' ', line.Length))
                            {
                                sb.Append(@char);
                            }

                            sb.Append('\r');
                            linePrefix = sb.ToString();
                        }
                    }

                    var test = await Task.WhenAny(tests);
                    tests.Remove(test);
                    var (vmSize, result) = await test;
                    RemoveTestFromQuota(vmSize, context);

                    switch (result)
                    {
                        case VerifyVMIResult.Use:
                            vms.AddRange(vmSize);
                            break;

                        case VerifyVMIResult.Retry:
                            if (++retryCounts[vmSize.Key] > 3)
                            {
                                lock (lockObj)
                                {
                                    Console.ForegroundColor = ConsoleColor.Yellow;
                                    Console.WriteLine(linePrefix + $"Skipping {vmSize.Key} because retry attempts were exhausted.");
                                    linePrefix = string.Empty;
                                    Console.ResetColor();
                                }

                                break;
                            }
                            vmsToTest.Add(vmSize);
                            Interlocked.Increment(ref count);
                            break;

                        case VerifyVMIResult.NextRegion:
                            vmsForNextRegion.Add(vmSize);
                            break;
                    }

                    loadedTests = vmsToTest.Where(vmSize => AddTestToQuotaIfQuotaPermits(vmSize, context)).ToList();
                }

                if (!vmsForNextRegion.Any())
                {
                    break;
                }
            }

            if (vmsForNextRegion.Any())
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine(linePrefix + "Due to either availability, capacity, quota, or other constraints, the following SKUs were not validated and will not be included:");
                linePrefix = string.Empty;

                foreach (var name in vmsForNextRegion.Select(vmsize => vmsize.Key).OrderBy(name => name, StringComparer.OrdinalIgnoreCase))
                {
                    Console.WriteLine($"    '{name}' with available in regions '{string.Join("', '", regionsForVm[name])}'.");
                }

                Console.ResetColor();
            }

            var batchVmInfo = vms
                .OrderBy(x => x!.VmSize)
                .ThenBy(x => x!.LowPriority)
                .ToList();

            Console.WriteLine(linePrefix + $"SupportedSkuCount:{batchVmInfo.Count}");
            linePrefix = string.Empty;

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

            var generations = sku.Capabilities.Where(x => x.Name.Equals("HyperVGenerations"))
                .SingleOrDefault()?.Value?.Split(",").ToList() ?? new();

            var vCpusAvailable = TryParseInt32("vCPUsAvailable");
            var encryptionAtHostSupported = TryParseBoolean("EncryptionAtHostSupported");
            var lowPriorityCapable = TryParseBoolean("LowPriorityCapable") ?? false;
            var maxDataDiskCount = TryParseInt32("MaxDataDiskCount");
            var maxResourceVolumeMB = TryParseInt32("MaxResourceVolumeMB");
            var memoryGB = TryParseDouble("MemoryGB");

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

            bool? TryParseBoolean(string name)
                => bool.TryParse(sku!.Capabilities.SingleOrDefault(x => x.Name.Equals(name, StringComparison.OrdinalIgnoreCase))?.Value, out var value) ? value : null;

            int? TryParseInt32(string name)
                => int.TryParse(sku!.Capabilities.SingleOrDefault(x => x.Name.Equals(name, StringComparison.OrdinalIgnoreCase))?.Value, System.Globalization.NumberFormatInfo.InvariantInfo, out var value) ? value : null;

            double? TryParseDouble(string name)
                => double.TryParse(sku!.Capabilities.SingleOrDefault(x => x.Name.Equals(name, StringComparison.OrdinalIgnoreCase))?.Value, System.Globalization.NumberFormatInfo.InvariantInfo, out var value) ? value : null;
        }

        private record class TestContext(int PoolQuota, int LowPriorityCoreQuota, int DedicatedCoreQuota, bool IsDedicatedCoreQuotaPerVmFamilyEnforced, Dictionary<string, int> DedicatedCoreQuotaPerVmFamily, Dictionary<string, int> DedicatedCoresInUseByVmFamily)
        {
            public int PoolCount { get; set; }
            public int LowPriorityCoresInUse { get; set; }
            public int DedicatedCoresInUse { get; set; }
        }

        private static async ValueTask<TestContext> GetTestQuotaContext(BatchAccountInfo batchAccount, IEnumerable<IGrouping<string, VirtualMachineInformation>> vmsizes, CancellationToken cancellationToken)
        {
            var vmSizeInfo = vmsizes.ToDictionary(vmsize => vmsize.Key, vmsize => vmsize.Last(), StringComparer.OrdinalIgnoreCase);

            var count = 0;
            var lowPriorityCoresInUse = 0;
            var dedicatedCoresInUse = 0;
            Dictionary<string, int> dedicatedCoresInUseByVmFamily = new(StringComparer.OrdinalIgnoreCase);

            await foreach (var (vmsize, dedicated, lowPriority) in batchAccount.Client.PoolOperations
                .ListPools(detailLevel: new Microsoft.Azure.Batch.ODATADetailLevel(selectClause: "id,vmSize")).ToAsyncEnumerable()
                .Join(batchAccount.Client.PoolOperations.ListPoolNodeCounts().ToAsyncEnumerable(),
                    pool => pool.Id,
                    counts => counts.PoolId,
                    (pool, counts) => (pool.VirtualMachineSize, counts.Dedicated, counts.LowPriority),
                    StringComparer.OrdinalIgnoreCase)
                .WithCancellation(cancellationToken))
            {
                ++count;
                var info = vmSizeInfo[vmsize];
                var vmFamily = info.VmFamily;
                var coresPerNode = info.VCpusAvailable ?? 200;

                lowPriorityCoresInUse += ((lowPriority?.Total ?? 0) - (lowPriority?.Preempted ?? 0)) * coresPerNode;
                var dedicatedCores = (dedicated?.Total ?? 0) * coresPerNode;
                dedicatedCoresInUse += dedicatedCores;
                if (!dedicatedCoresInUseByVmFamily.TryAdd(vmFamily, dedicatedCores))
                {
                    dedicatedCoresInUseByVmFamily[vmFamily] += dedicatedCores;
                }
            }

            return new(
                batchAccount.PoolQuota ?? int.MaxValue,
                batchAccount.LowPriorityCoreQuota ?? int.MaxValue,
                batchAccount.DedicatedCoreQuota ?? int.MaxValue,
                batchAccount.IsDedicatedCoreQuotaPerVmFamilyEnforced ?? false,
                batchAccount.DedicatedCoreQuotaPerVmFamily.ToDictionary(quota => quota.Name, quota => quota.CoreQuota ?? int.MaxValue, StringComparer.OrdinalIgnoreCase),
                dedicatedCoresInUseByVmFamily)
            {
                PoolCount = count,
                LowPriorityCoresInUse = lowPriorityCoresInUse,
                DedicatedCoresInUse = dedicatedCoresInUse,
            };
        }

        private static bool AddTestToQuotaIfQuotaPermits(IGrouping<string, VirtualMachineInformation> vmSize, TestContext context)
        {
            var info = vmSize.Last();
            var family = info.VmFamily;
            var coresPerNode = info.VCpusAvailable ?? 200;
            var isLowPriority = info.LowPriority;

            if (context.PoolCount + 1 > context.PoolQuota ||
                (isLowPriority && context.LowPriorityCoresInUse + coresPerNode > context.LowPriorityCoreQuota) ||
                (!isLowPriority && context.DedicatedCoresInUse + coresPerNode > context.DedicatedCoreQuota) ||
                (!isLowPriority && context.IsDedicatedCoreQuotaPerVmFamilyEnforced && (context.DedicatedCoresInUseByVmFamily.TryGetValue(family, out var inUse) ? inUse : 0) > (context.DedicatedCoreQuotaPerVmFamily.TryGetValue(family, out var quota) ? quota : 0)))
            {
                return false;
            }

            context.PoolCount += 1;

            if (isLowPriority)
            {
                context.LowPriorityCoresInUse += coresPerNode;
            }
            else
            {
                context.DedicatedCoresInUse += coresPerNode;

                if (context.IsDedicatedCoreQuotaPerVmFamilyEnforced)
                {
                    if (!context.DedicatedCoresInUseByVmFamily.TryAdd(family, coresPerNode))
                    {
                        context.DedicatedCoresInUseByVmFamily[family] += coresPerNode;
                    }
                }
            }

            return true;
        }

        private static void RemoveTestFromQuota(IGrouping<string, VirtualMachineInformation> vmSize, TestContext context)
        {
            var info = vmSize.Last();
            var family = info.VmFamily;
            var coresPerNode = info.VCpusAvailable ?? 200;
            var isLowPriority = info.LowPriority;

            context.PoolCount -= 1;

            if (isLowPriority)
            {
                context.LowPriorityCoresInUse -= coresPerNode;
            }
            else
            {
                context.DedicatedCoresInUse -= coresPerNode;

                if (context.IsDedicatedCoreQuotaPerVmFamilyEnforced)
                {
                    context.DedicatedCoresInUseByVmFamily[family] -= coresPerNode;
                }
            }
        }

        private async Task<VerifyVMIResult> TestVMSizeInBatchAsync(IGrouping<string, VirtualMachineInformation> vmSize, BatchAccountInfo accountInfo, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(vmSize);

            var name = vmSize.Key;
            var vm = vmSize.Last();

            if (vm.RegionsAvailable.Contains(accountInfo.Location.Name))
            {
                var pool = accountInfo.Client.PoolOperations.CreatePool(name, name, GetMachineConfiguration(vm.HyperVGenerations.Contains("V2"), vm.EncryptionAtHostSupported ?? false), targetDedicatedComputeNodes: vm.LowPriority ? 0 : 1, targetLowPriorityComputeNodes: vm.LowPriority ? 1 : 0);
                pool.TargetNodeCommunicationMode = Microsoft.Azure.Batch.Common.NodeCommunicationMode.Simplified;
                pool.ResizeTimeout = TimeSpan.FromMinutes(30);
                pool.NetworkConfiguration = new() { PublicIPAddressConfiguration = new(Microsoft.Azure.Batch.Common.IPAddressProvisioningType.BatchManaged), SubnetId = accountInfo.SubnetId };

                try
                {
                    await pool.CommitAsync(cancellationToken: cancellationToken);

                    do
                    {
                        await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
                        await pool.RefreshAsync(detailLevel: new Microsoft.Azure.Batch.ODATADetailLevel(selectClause: "allocationState,id,resizeErrors"), cancellationToken: cancellationToken);
                    }
                    while (Microsoft.Azure.Batch.Common.AllocationState.Steady != pool.AllocationState);

                    if (pool.ResizeErrors?.Any() ?? false)
                    {
                        if (pool.ResizeErrors.Any(e =>
                            Microsoft.Azure.Batch.Common.PoolResizeErrorCodes.UnsupportedVMSize.Equals(e.Code, StringComparison.OrdinalIgnoreCase)
                            || (!vm.LowPriority && @"AccountVMSeriesCoreQuotaReached".Equals(e.Code, StringComparison.OrdinalIgnoreCase))))
                        {
                            return VerifyVMIResult.NextRegion;  // Dedicated vm family quota. Try another batch account.
                        }
                        else if (pool.ResizeErrors.Any(e =>
                            Microsoft.Azure.Batch.Common.PoolResizeErrorCodes.AccountCoreQuotaReached.Equals(e.Code, StringComparison.OrdinalIgnoreCase) ||
                            Microsoft.Azure.Batch.Common.PoolResizeErrorCodes.AccountLowPriorityCoreQuotaReached.Equals(e.Code, StringComparison.OrdinalIgnoreCase) ||
                            Microsoft.Azure.Batch.Common.PoolResizeErrorCodes.AccountSpotCoreQuotaReached.Equals(e.Code, StringComparison.OrdinalIgnoreCase)))
                        {
                            return VerifyVMIResult.Retry; // Either a timing issue or other concurrent use of the batch account. Try again.
                        }
                        else
                        {
                            var errorCodes = pool.ResizeErrors!.Select(e => e.Code).Distinct().ToList();
                            var isAllocationFailed = errorCodes.Count == 1 && errorCodes.Contains(Microsoft.Azure.Batch.Common.PoolResizeErrorCodes.AllocationFailed);
                            var isAllocationTimedOut = errorCodes.Count == 1 && errorCodes.Contains(Microsoft.Azure.Batch.Common.PoolResizeErrorCodes.AllocationTimedOut);
                            var isOverconstrainedAllocationRequest = errorCodes.Count == 1 && errorCodes.Contains(Microsoft.Azure.Batch.Common.PoolResizeErrorCodes.OverconstrainedAllocationRequestError);

                            if (isAllocationTimedOut)
                            {
                                return VerifyVMIResult.Retry;
                            }

                            if (isAllocationFailed)
                            {
                                var values = pool.ResizeErrors!
                                    .SelectMany(e => e.Values ?? new List<Microsoft.Azure.Batch.NameValuePair>())
                                    .ToDictionary(pair => pair.Name, pair => pair.Value, StringComparer.OrdinalIgnoreCase);

                                if (values.TryGetValue("Reason", out var reason))
                                {
                                    if ("The server encountered an internal error.".Equals(reason, StringComparison.OrdinalIgnoreCase))
                                    {
                                        // https://learn.microsoft.com/troubleshoot/azure/general/azure-batch-pool-resizing-failure#symptom-for-scenario-4
                                        return VerifyVMIResult.Retry;
                                        // TODO: count retries for this vmsize and return .NextRegion
                                    }
                                    else if ("Allocation failed as subnet has delegation to external resources.".Equals(reason, StringComparison.OrdinalIgnoreCase))
                                    {
                                        // https://learn.microsoft.com/troubleshoot/azure/general/azure-batch-pool-resizing-failure#symptom-for-scenario-1
                                    }
                                }

                                if (values.TryGetValue("Provider Error Json Truncated", out var isTruncatedString) && bool.TryParse(isTruncatedString, out var isTruncated))
                                {
                                    // Based on https://learn.microsoft.com/troubleshoot/azure/general/azure-batch-pool-resizing-failure#symptom-for-scenario-2

                                    if (!isTruncated && values.TryGetValue("Provider Error Json", out var errorJsonString))
                                    {
                                        var error = JsonSerializer.Deserialize<ProviderError>(errorJsonString);

                                        if ("InternalServerError".Equals(error.Error.Code, StringComparison.OrdinalIgnoreCase))
                                        {
                                            return VerifyVMIResult.Retry;
                                            // TODO: count retries for this vmsize and return .NextRegion
                                        }
                                    }
                                }
                            }

                            lock (lockObj)
                            {
                                Console.ForegroundColor = ConsoleColor.Yellow;
                                Console.WriteLine(linePrefix + $"Skipping {name} because the batch account failed to allocate nodes into a pool due to '{string.Join("', '", pool.ResizeErrors!.Select(e => e.Code))}'.");
                                linePrefix = string.Empty;

                                if (!isOverconstrainedAllocationRequest)
                                {
                                    Console.WriteLine($"    Additional information: Message: '{string.Join("', '", pool.ResizeErrors!.Select(e => e.Message))}'");

                                    foreach (var detail in pool.ResizeErrors!.SelectMany(e => e.Values ?? Enumerable.Empty<Microsoft.Azure.Batch.NameValuePair>()))
                                    {
                                        Console.WriteLine($"    '{detail.Name}': '{detail.Value}'.");
                                    }
                                }

                                Console.ResetColor();
                            }

                            return VerifyVMIResult.Skip;
                        }
                    }

                    List<Microsoft.Azure.Batch.ComputeNode> nodes;

                    do
                    {
                        await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                        nodes = await pool.ListComputeNodes(detailLevel: new Microsoft.Azure.Batch.ODATADetailLevel(selectClause: "id,state,errors")).ToAsyncEnumerable().ToListAsync(cancellationToken);

                        if (nodes.Any(n => n.Errors?.Any() ?? false))
                        {
                            lock (lockObj)
                            {
                                Console.ForegroundColor = ConsoleColor.Yellow;
                                Console.WriteLine(linePrefix + $"Skipping {name} due to node(s) failing to start due to '{string.Join("', '", nodes.SelectMany(n => n.Errors).Select(e => e.Code))}'.");
                                linePrefix = string.Empty;
                                Console.WriteLine($"    Additional information: Message: '{string.Join("', '", nodes.SelectMany(n => n.Errors).Select(e => e.Message))}'");

                                foreach (var detail in nodes.SelectMany(n => n.Errors).SelectMany(e => e.ErrorDetails ?? Enumerable.Empty<Microsoft.Azure.Batch.NameValuePair>()))
                                {
                                    Console.WriteLine($"    '{detail.Name}': '{detail.Value}'.");
                                }

                                Console.ResetColor();
                            }

                            return VerifyVMIResult.Skip;
                        }

                        if (nodes.Any(n => Microsoft.Azure.Batch.Common.ComputeNodeState.Unusable == n.State
                            || Microsoft.Azure.Batch.Common.ComputeNodeState.Unknown == n.State))
                        {
                            lock (lockObj)
                            {
                                Console.ForegroundColor = ConsoleColor.Yellow;
                                Console.WriteLine(linePrefix + $"Skipping {name} due to node(s) becoming unusable without providing errors.");
                                linePrefix = string.Empty;
                                Console.ResetColor();
                            }

                            return VerifyVMIResult.Skip;
                        }
                    }
                    while (nodes.Any(n => Microsoft.Azure.Batch.Common.ComputeNodeState.Idle != n.State
                        && Microsoft.Azure.Batch.Common.ComputeNodeState.Preempted != n.State
                        && Microsoft.Azure.Batch.Common.ComputeNodeState.LeavingPool != n.State));
                }
                catch (Microsoft.Azure.Batch.Common.BatchException exception)
                {
                    if (exception.StackTrace?.Contains(" at Microsoft.Azure.Batch.CloudPool.CommitAsync(IEnumerable`1 additionalBehaviors, CancellationToken cancellationToken)") ?? false)
                    {
                        pool = default;

                        if (System.Net.HttpStatusCode.InternalServerError == exception.RequestInformation.HttpStatusCode)
                        {
                            lock (lockObj)
                            {
                                Console.ForegroundColor = ConsoleColor.Yellow;
                                Console.WriteLine(linePrefix + $"Retrying {name} due to 'InternalServerError' failure when creating the pool.");
                                linePrefix = string.Empty;
                                var error = exception.RequestInformation.BatchError;
                                Console.WriteLine($"    '{error.Code}'({error.Message.Language}): '{error.Message.Value}'");

                                foreach (var value in error.Values ?? Enumerable.Empty<Microsoft.Azure.Batch.BatchErrorDetail>())
                                {
                                    Console.WriteLine($"    '{value.Key}': '{value.Value}'");
                                }

                                Console.ResetColor();
                            }

                            return VerifyVMIResult.Retry;
                        }
                        else if (exception.RequestInformation.HttpStatusCode is null || exception.RequestInformation.HttpStatusCode == System.Net.HttpStatusCode.GatewayTimeout)
                        {
                            return VerifyVMIResult.Retry;
                        }
                        else
                        {
                            lock (lockObj)
                            {
                                Console.ForegroundColor = ConsoleColor.Yellow;
                                Console.WriteLine(linePrefix + $"Deferring {name} to next region due to '{exception.RequestInformation.HttpStatusCode}' failure when creating the pool.");
                                linePrefix = string.Empty;
                                Console.WriteLine("    Please check and delete pool manually.");
                                Console.ResetColor();
                            }

                            return VerifyVMIResult.NextRegion;
                        }
                    }
                }
                finally
                {
                    while (pool is not null)
                    {
                        try
                        {
                            await ResetOnAccess(ref pool).DeleteAsync(cancellationToken: CancellationToken.None);
                        }
                        catch (Microsoft.Azure.Batch.Common.BatchException exception)
                        {
                            switch (exception.RequestInformation.HttpStatusCode)
                            {
                                case System.Net.HttpStatusCode.Conflict:
                                    break;

                                default:
                                    lock (lockObj)
                                    {
                                        Console.ForegroundColor = ConsoleColor.Red;
                                        Console.WriteLine(linePrefix + $"Failed to delete pool '{name}' on '{accountInfo.Name}' ('{exception.RequestInformation.HttpStatusCode?.ToString("G") ?? "[no response]"}') {exception.GetType().FullName}: {exception.Message}");
                                        linePrefix = string.Empty;
                                        Console.WriteLine("    Please check and delete pool manually.");
                                        Console.ResetColor();
                                        Console.WriteLine();
                                    }
                                    break;
                            }
                        }
                        catch (InvalidOperationException exception)
                        {
                            if ("This operation is forbidden on unbound objects.".Equals(exception.Message, StringComparison.OrdinalIgnoreCase))
                            {
                                try
                                {
                                    pool = await accountInfo.Client.PoolOperations.GetPoolAsync(name, new Microsoft.Azure.Batch.ODATADetailLevel(selectClause: "id"), cancellationToken: CancellationToken.None);
                                }
                                catch (Exception ex)
                                {
                                    lock (lockObj)
                                    {
                                        Console.ForegroundColor = ConsoleColor.Red;
                                        Console.WriteLine(linePrefix + $"Failed to delete pool '{name}' on '{accountInfo.Name}'. Attempting to locate the pool resulted in {ex.GetType().FullName}: {ex.Message}");
                                        linePrefix = string.Empty;
                                        Console.WriteLine("    Please check and delete pool manually.");
                                        Console.ResetColor();
                                        Console.WriteLine();
                                    }
                                }
                            }
                            else
                            {
                                lock (lockObj)
                                {
                                    Console.ForegroundColor = ConsoleColor.Red;
                                    Console.WriteLine(linePrefix + $"Failed to delete pool '{name}' on '{accountInfo.Name}' {exception.GetType().FullName}: {exception.Message}");
                                    linePrefix = string.Empty;
                                    Console.WriteLine("    Please check and delete pool manually.");
                                    Console.ResetColor();
                                    Console.WriteLine();
                                }
                            }
                        }
                    }
                }
            }
            else
            {
                return VerifyVMIResult.NextRegion;
            }

            return VerifyVMIResult.Use;
        }

        private static T ResetOnAccess<T>(ref T? value)
        {
            ArgumentNullException.ThrowIfNull(value);

            var result = value;
            value = default;
            return result;
        }

        private struct ProviderError
        {
            [JsonPropertyName("error")]
            public ErrorRecord Error { get; set; }

            public struct ErrorRecord
            {
                [JsonPropertyName("code")]
                public string Code { get; set; }

                [JsonPropertyName("message")]
                public string Message { get; set; }
            }
        }

        private sealed class BatchAccountInfo : IDisposable
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
