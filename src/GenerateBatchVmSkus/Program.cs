// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Reflection;
using System.Text;
using System.Text.Json;
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
        public string[]? BatchAccount { get; set; }

        public static Configuration BuildConfiguration(string[] args)
        {
            var configBuilder = new ConfigurationBuilder();

            var configurationSource = configBuilder.AddCommandLine(args).Build();
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
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.GetType().FullName + " " + ex.Message);
                Console.ResetColor();

                var exitCode = ex.HResult;
                if (exitCode == 0) { exitCode = 1; }
                if (Environment.OSVersion.Platform == PlatformID.Unix) { exitCode &= 0x0ff; }
                Environment.Exit(exitCode);
            }
        }

        private readonly ConcurrentDictionary<string, ImmutableHashSet<string>> regionsForVm = new(StringComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<string, VirtualMachineSize> sizeForVm = new(StringComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<string, ComputeResourceSku> skuForVm = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, PricingItem> priceForVm = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, PricingItem> lowPrPriceForVm = new(StringComparer.OrdinalIgnoreCase);
        private readonly CancellationTokenSource cancellationTokenSource = new();

        private void CancelKeyPress(object? sender, ConsoleCancelEventArgs e)
        {
            e.Cancel = true;
            cancellationTokenSource.Cancel();
        }

        private static double ConvertMiBToGiB(int value) => Math.Round(value / 1024.0, 2);

        private record struct NameWithIndex(string Name, int Index);
        private record struct ItemWithName<T>(string Name, T Item);

        private static IAsyncEnumerable<BatchAccountInfo> GetBatchAccountsAsync(Azure.ResourceManager.Resources.SubscriptionResource subscription, TokenCredential credential, Configuration configuration, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(configuration?.BatchAccount, nameof(configuration));

            var namesInOrder = configuration.BatchAccount.Select((a, i) => new NameWithIndex(a, i)).ToList();
            if (namesInOrder.Any(account => string.IsNullOrWhiteSpace(account.Name)))
            {
                throw new ArgumentException("Batch account name is missing.", nameof(configuration));
            }

            var accountNames = namesInOrder
                .GroupBy(s => s.Name, StringComparer.OrdinalIgnoreCase)
                .ToLookup(g => g.Key, g => g.Select(i => i.Index).Single(), StringComparer.OrdinalIgnoreCase);

            if (accountNames.Any(group => group.Count() != 1))
            {
                throw new ArgumentException("Duplicate batch account names provided.", nameof(configuration));
            }

            if (accountNames.Count != namesInOrder.Count)
            {
                throw new ArgumentException("Requested batch account not found.", nameof(configuration));
            }

            try
            {
                return subscription.GetBatchAccountsAsync(cancellationToken: cancellationToken)
                    .Where(resource => accountNames.Contains(resource.Id.Name))
                    .OrderBy(resource => accountNames[resource.Id.Name])
                    .SelectAwaitWithCancellation(async (resource, token) => new ItemWithName<Response<BatchAccountResource>>(resource.Id.Name, await resource.GetAsync(token)))
                    .Select((resource, index) => new BatchAccountInfo(resource.Item.Value?.Data ?? throw new InvalidOperationException($"Batch account {resource.Name} not retrieved."), index, credential, cancellationToken));
            }
            catch (InvalidOperationException exception)
            {
                throw new ArgumentException("Batch account with duplicate name found in subscription.", nameof(configuration), exception);
            }
        }

        private async Task<int> RunAsync(Configuration configuration)
        {
            ArgumentException.ThrowIfNullOrEmpty(configuration.OutputFilePath);
            ArgumentException.ThrowIfNullOrEmpty(configuration.SubscriptionId);
            ArgumentNullException.ThrowIfNull(configuration.BatchAccount);

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
                        List<VirtualMachineSize>? sizes = null;
                        List<ComputeResourceSku>? skus = null;
                        var count = 0;

                        await foreach (var vm in subscription.GetBatchSupportedVirtualMachineSkusAsync(region, cancellationToken: token).Select(s => s.Name).WithCancellation(token))
                        {
                            ++count;
                            _ = regionsForVm.AddOrUpdate(vm, _ => ImmutableHashSet<string>.Empty.Add(region.Name), (_, value) => value.Add(region.Name));

                            sizes ??= await subscription.GetVirtualMachineSizesAsync(region, cancellationToken: token).ToListAsync(token);
                            _ = sizeForVm.GetOrAdd(vm, vm =>
                                sizes.Single(vmsize => vmsize.Name.Equals(vm, StringComparison.OrdinalIgnoreCase)));

                            skus ??= await subscription.GetComputeResourceSkusAsync($"location eq '{region.Name}'", cancellationToken: token).ToListAsync(token);
                            _ = skuForVm.GetOrAdd(vm, vm =>
                                skus.Single(sku => sku.Name.Equals(vm, StringComparison.OrdinalIgnoreCase)));
                        }

                        Console.WriteLine($"{region.Name} supportedSkuCount:{count}");
                    }
                    catch (RequestFailedException e)
                    {
                        Console.WriteLine($"No skus supported in {region.Name}. {e.ErrorCode}");
                    }
                });

            var batchSupportedVmSet = regionsForVm.Keys;
            Console.WriteLine($"Superset supportedSkuCount:{batchSupportedVmSet.Count}");

            ConcurrentBag<VirtualMachineInformation> vms = new();
            Console.WriteLine("Testing each SKU...");
            _ = Parallel.ForEach(batchSupportedVmSet, vmsForNextRegion.Add);
            vmiParallelOptions.CancellationToken = cancellationTokenSource.Token;

            await foreach (var batchAccount in batchAccounts.WithCancellation(cancellationTokenSource.Token))
            {
                using var disposable = batchAccount;
                batchSupportedVmSet = vmsForNextRegion.ToList();
                vmsForNextRegion.Clear();
                count = batchSupportedVmSet.Count;
                index = 0;
                Console.WriteLine($"Verifying {batchSupportedVmSet.Count} SKUs with {batchAccount.Name}.");
                await Parallel.ForEachAsync(batchSupportedVmSet, vmiParallelOptions, async (name, token) =>
                    await GetVirtualMachineInformationsAsync(name, batchAccount).ForEachAsync(vm => vms.Add(vm), token));

                if (!vmsForNextRegion.Any())
                {
                    break;
                }
            }

            if (vmsForNextRegion.Any())
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("Due to either availability, capacity, quota, or other constraints, the following SKUs were not validated:");

                foreach (var name in vmsForNextRegion)
                {
                    Console.WriteLine($"    '{name}', available in regions: '{string.Join("', '", regionsForVm[name])}'.");
                }

                Console.ResetColor();
            }

            var batchVmInfo = vms
                .OrderBy(x => x!.VmSize)
                .ThenBy(x => x!.LowPriority)
                .ToList();

            Console.WriteLine($"SupportedSkuCount:{batchVmInfo.Count}");

            var jsonOptions = new JsonSerializerOptions(JsonSerializerDefaults.General)
            {
                WriteIndented = true
            };

            var data = JsonSerializer.Serialize(batchVmInfo, options: jsonOptions);
            await File.WriteAllTextAsync(configuration.OutputFilePath!, data, cancellationTokenSource.Token);
            return 0;
        }

        private readonly ParallelOptions vmiParallelOptions = new() { MaxDegreeOfParallelism = 3 };
        private int index;
        private int count;
        private readonly object lockObj = new();
        private readonly ConcurrentBag<string> vmsForNextRegion = new();
        private string linePrefix = string.Empty;

        private async IAsyncEnumerable<VirtualMachineInformation> GetVirtualMachineInformationsAsync(string name, BatchAccountInfo accountInfo)
        {
            if (name is null)
            {
                yield break;
            }

            if (!Console.IsOutputRedirected)
            {
                lock (lockObj)
                {
                    var line = $"[{accountInfo.Name}:{accountInfo.Index}] Testing SKUs {Math.Max(0, Interlocked.Increment(ref index) - vmiParallelOptions.MaxDegreeOfParallelism) / (double)count:P2} complete...";
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

            var sizeInfo = sizeForVm[name];
            var sku = skuForVm[name];

            if (sizeInfo is null || sizeInfo.MemoryInMB is null || sizeInfo.ResourceDiskSizeInMB is null)
            {
                throw new Exception($"Size info is null for VM {name}");
            }

            if (sku is null)
            {
                throw new Exception($"Sku info is null for VM {name}");
            }

            var generations = sku.Capabilities.Where(x => x.Name.Equals("HyperVGenerations"))
                .SingleOrDefault()?.Value?.Split(",").ToList() ?? new();

            _ = int.TryParse(sku.Capabilities.Where(x => x.Name.Equals("vCPUsAvailable")).SingleOrDefault()?.Value, out var vCpusAvailable);
            _ = bool.TryParse(sku.Capabilities.Where(x => x.Name.Equals("EncryptionAtHostSupported")).SingleOrDefault()?.Value, out var encryptionAtHostSupported);
            _ = bool.TryParse(sku.Capabilities.Where(x => x.Name.Equals("LowPriorityCapable")).SingleOrDefault()?.Value, out var lowPriorityCapable);

            lock (lockObj)
            {
                Console.ForegroundColor = ConsoleColor.Green;

                if (int.TryParse(sku.Capabilities.Where(x => x.Name.Equals("MaxDataDiskCount")).SingleOrDefault()?.Value, out var dcount))
                {
                    if (dcount != sizeInfo.MaxDataDiskCount) { Console.WriteLine(linePrefix + $"{name}: size::MaxDataDiskCount: {sizeInfo.MaxDataDiskCount} sku::MaxDataDiskCount: {dcount}"); }
                }
                else { Console.WriteLine(linePrefix + $"Capabilities does not contain MaxDataDiskCount"); }

                if (int.TryParse(sku.Capabilities.Where(x => x.Name.Equals("MaxResourceVolumeMB")).SingleOrDefault()?.Value, out var rdisk))
                {
                    if (rdisk != sizeInfo.ResourceDiskSizeInMB) { Console.WriteLine(linePrefix + $"{name}: size::ResourceDiskSizeInMB: {sizeInfo.ResourceDiskSizeInMB} sku::MaxResourceVolumeMB: {rdisk}"); }
                }
                else { Console.WriteLine(linePrefix + $"Capabilities does not contain MaxResourceVolumeMB"); }

                if (double.TryParse(sku.Capabilities.Where(x => x.Name.Equals("MemoryGB")).SingleOrDefault()?.Value, System.Globalization.CultureInfo.InvariantCulture, out var nmem))
                {
                    if (nmem != ConvertMiBToGiB(sizeInfo.MemoryInMB.Value)) { Console.WriteLine(linePrefix + $"{name}: size::MemoryInMB: {sizeInfo.MemoryInMB} sku::MemoryGB: {nmem}"); }
                }
                else { Console.WriteLine(linePrefix + $"Capabilities does not contain MemoryGB"); }

                Console.ResetColor();
            }

            if (regionsForVm[name].Contains(accountInfo.Location.Name))
            {
                var pool = accountInfo.Client.PoolOperations.CreatePool(sku.Name, name, GetMachineConfiguration(generations.Contains("V2"), encryptionAtHostSupported), targetDedicatedComputeNodes: lowPriorityCapable ? 0 : 1, targetLowPriorityComputeNodes: lowPriorityCapable ? 1 : 0);
                pool.TargetNodeCommunicationMode = Microsoft.Azure.Batch.Common.NodeCommunicationMode.Simplified;
                pool.ResizeTimeout = TimeSpan.FromMinutes(30);

                try
                {
                    await pool.CommitAsync(cancellationToken: cancellationTokenSource.Token);

                    do
                    {
                        await Task.Delay(TimeSpan.FromSeconds(30), cancellationTokenSource.Token);
                        await pool.RefreshAsync(detailLevel: new Microsoft.Azure.Batch.ODATADetailLevel(selectClause: "allocationState,id,resizeErrors"), cancellationToken: cancellationTokenSource.Token);
                    }
                    while (Microsoft.Azure.Batch.Common.AllocationState.Steady != pool.AllocationState);

                    if (pool.ResizeErrors?.Any() ?? false)
                    {
                        lock (lockObj)
                        {
                            Console.ForegroundColor = ConsoleColor.Yellow;
                            Console.WriteLine(linePrefix + $"Skipping {name} because the batch account failed to allocate nodes into a pool due to '{string.Join("', '", pool.ResizeErrors.Select(e => e.Code))}'.");
                            Console.WriteLine($"    Additional information: Message: '{string.Join("', '", pool.ResizeErrors.Select(e => e.Message))}'");

                            foreach (var detail in pool.ResizeErrors.SelectMany(e => e.Values ?? new List<Microsoft.Azure.Batch.NameValuePair>()))
                            {
                                Console.WriteLine($"    '{detail.Name}': '{detail.Value}'.");
                            }

                            Console.ResetColor();
                            linePrefix = string.Empty;
                        }

                        if (pool.ResizeErrors?.Any(e => Microsoft.Azure.Batch.Common.PoolResizeErrorCodes.UnsupportedVMSize.Equals(e.Code, StringComparison.OrdinalIgnoreCase)
                            || (lowPriorityCapable && @"AccountVMSeriesCoreQuotaReached".Equals(e.Code, StringComparison.OrdinalIgnoreCase))) ?? false)
                        {
                            vmsForNextRegion.Add(name);
                        }

                        yield break;
                    }

                    List<Microsoft.Azure.Batch.ComputeNode> nodes;

                    do
                    {
                        await Task.Delay(TimeSpan.FromSeconds(5), cancellationTokenSource.Token);
                        nodes = await pool.ListComputeNodes(detailLevel: new Microsoft.Azure.Batch.ODATADetailLevel(selectClause: "id,state,errors")).ToAsyncEnumerable().ToListAsync(cancellationTokenSource.Token);

                        if (nodes.Any(n => n.Errors?.Any() ?? false))
                        {
                            lock (lockObj)
                            {
                                Console.ForegroundColor = ConsoleColor.Yellow;
                                Console.WriteLine(linePrefix + $"Skipping {name} because node(s) failed to start in our batch account's pool due to '{string.Join("', '", nodes.SelectMany(n => n.Errors).Select(e => e.Code))}'.");
                                Console.WriteLine($"    Additional information: Message: '{string.Join("', '", nodes.SelectMany(n => n.Errors).Select(e => e.Message))}'");

                                foreach (var detail in nodes.SelectMany(n => n.Errors).SelectMany(e => e.ErrorDetails ?? new List<Microsoft.Azure.Batch.NameValuePair>()))
                                {
                                    Console.WriteLine($"    '{detail.Name}': '{detail.Value}'.");
                                }

                                Console.ResetColor();
                                linePrefix = string.Empty;
                            }

                            yield break;
                        }

                        if (nodes.Any(n => Microsoft.Azure.Batch.Common.ComputeNodeState.Unusable == n.State
                            || Microsoft.Azure.Batch.Common.ComputeNodeState.Unknown == n.State))
                        {
                            lock (lockObj)
                            {
                                Console.ForegroundColor = ConsoleColor.Yellow;
                                Console.WriteLine(linePrefix + $"Skipping {name} because node(s) became unusable without providing errors.");
                                Console.ResetColor();
                                linePrefix = string.Empty;
                            }

                            yield break;
                        }
                    }
                    while (nodes.Any(n => Microsoft.Azure.Batch.Common.ComputeNodeState.Idle != n.State
                        && Microsoft.Azure.Batch.Common.ComputeNodeState.Preempted != n.State
                        && Microsoft.Azure.Batch.Common.ComputeNodeState.LeavingPool != n.State));
                }
                finally
                {
                    while (pool is not null)
                    {
                        try
                        {
                            var poolToDelete = pool;
                            pool = null;
                            await poolToDelete.DeleteAsync(cancellationToken: CancellationToken.None);
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
                                        Console.WriteLine(linePrefix + $"Failed to delete '{name}' ('{exception.RequestInformation.HttpStatusCode?.ToString("G") ?? "[no response]"}') {exception.GetType().FullName}: {exception.Message}");
                                        Console.ResetColor();
                                        linePrefix = string.Empty;
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
                                    pool = await accountInfo.Client.PoolOperations.GetPoolAsync(sku.Name, new Microsoft.Azure.Batch.ODATADetailLevel(selectClause: "id"), cancellationToken: CancellationToken.None);
                                }
                                catch (Exception ex)
                                {
                                    lock (lockObj)
                                    {
                                        Console.ForegroundColor = ConsoleColor.Red;
                                        Console.WriteLine(linePrefix + $"Failed to delete '{name}'. Attempting to locate the pool resulted in {ex.GetType().FullName}: {ex.Message}");
                                        Console.ResetColor();
                                        linePrefix = string.Empty;
                                    }
                                }
                            }
                            else
                            {
                                lock (lockObj)
                                {
                                    Console.ForegroundColor = ConsoleColor.Red;
                                    Console.WriteLine(linePrefix + $"Failed to delete '{name}' {exception.GetType().FullName}: {exception.Message}");
                                    Console.ResetColor();
                                    linePrefix = string.Empty;
                                }
                            }
                        }
                    }
                }
            }
            else
            {
                vmsForNextRegion.Add(name);
                yield break;
            }

            yield return new()
            {
                MaxDataDiskCount = sizeInfo.MaxDataDiskCount,
                MemoryInGiB = ConvertMiBToGiB(sizeInfo.MemoryInMB!.Value),
                VCpusAvailable = vCpusAvailable,
                ResourceDiskSizeInGiB = ConvertMiBToGiB(sizeInfo.ResourceDiskSizeInMB!.Value),
                VmSize = sizeInfo.Name,
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
                    MaxDataDiskCount = sizeInfo.MaxDataDiskCount,
                    MemoryInGiB = ConvertMiBToGiB(sizeInfo.MemoryInMB!.Value),
                    VCpusAvailable = vCpusAvailable,
                    ResourceDiskSizeInGiB = ConvertMiBToGiB(sizeInfo.ResourceDiskSizeInMB!.Value),
                    VmSize = sizeInfo.Name,
                    VmFamily = sku.Family,
                    LowPriority = true,
                    HyperVGenerations = generations,
                    RegionsAvailable = regionsForVm[name].Order().ToList(),
                    EncryptionAtHostSupported = encryptionAtHostSupported,
                    PricePerHour = lowPrPriceForVm.TryGetValue(name, out var lowPrPriceItem) ? (decimal)lowPrPriceItem.retailPrice : null,
                };
            }
        }

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

        private sealed class BatchAccountInfo : IDisposable
        {
            private Microsoft.Azure.Batch.BatchClient ClientFactory(TokenCredential credential, CancellationToken cancellationToken)
            {
                return Microsoft.Azure.Batch.BatchClient.Open(
                new Microsoft.Azure.Batch.Auth.BatchTokenCredentials(
                    new UriBuilder(Uri.UriSchemeHttps, data.AccountEndpoint).Uri.AbsoluteUri,
                    async () => await BatchTokenProvider(credential, cancellationToken)));
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

            public BatchAccountInfo(BatchAccountData data, int index, TokenCredential credential, CancellationToken cancellationToken)
            {
                Index = index;
                this.data = data;
                batchClient = new Lazy<Microsoft.Azure.Batch.BatchClient>(() => ClientFactory(credential, cancellationToken));
            }

            public string Name => data.Name;

            public int Index { get; }

            public AzureLocation Location => data.Location!.Value;

            public Microsoft.Azure.Batch.BatchClient Client => batchClient.Value;

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
