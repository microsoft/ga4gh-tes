// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Concurrent;
using System.Collections.Immutable;
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
        public string? BatchAccount { get; set; }

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
                Environment.Exit(1);
            }

            Environment.Exit(await new Program().RunAsync(configuration));
        }

        private readonly ConcurrentDictionary<string, ImmutableHashSet<string>> regionsForVm = new(StringComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<string, VirtualMachineSize> sizeForVm = new(StringComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<string, ComputeResourceSku> skuForVm = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, PricingItem> priceForVm = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, PricingItem> lowPrPriceForVm = new(StringComparer.OrdinalIgnoreCase);
        private readonly CancellationTokenSource cancellationTokenSource = new();
        private AccessToken? accessToken;

        private void CancelKeyPress(object? sender, ConsoleCancelEventArgs e)
        {
            e.Cancel = true;
            cancellationTokenSource.Cancel();
        }

        private async Task<string> BatchTokenProvider(TokenCredential tokenCredential, CancellationToken cancellationToken)
        {
            if (accessToken?.ExpiresOn > DateTimeOffset.UtcNow.Subtract(TimeSpan.FromSeconds(60)))
            {
                return accessToken.Value.Token;
            }

            accessToken = await tokenCredential.GetTokenAsync(
                new(
                    new[] { new UriBuilder("https://batch.core.windows.net/") { Path = ".default" }.Uri.AbsoluteUri },
                    Guid.NewGuid().ToString("D")),
                cancellationToken);
            return accessToken.Value.Token;
        }

        private static double ConvertMiBToGiB(int value) => Math.Round(value / 1024.0, 2);

        private async Task<int> RunAsync(Configuration configuration)
        {
            ArgumentException.ThrowIfNullOrEmpty(configuration.OutputFilePath);
            ArgumentException.ThrowIfNullOrEmpty(configuration.SubscriptionId);
            ArgumentException.ThrowIfNullOrEmpty(configuration.BatchAccount);

            Console.CancelKeyPress += CancelKeyPress;

            Console.WriteLine("Starting...");
            TokenCredential tokenCredential = new DefaultAzureCredential();
            var client = new ArmClient(tokenCredential);
            var appCache = new MemoryCache(new MemoryCacheOptions());
            var cacheAndRetryHandler = new CachingRetryHandler(appCache, Options.Create(new RetryPolicyOptions()));
            var priceApiClient = new PriceApiClient(cacheAndRetryHandler, new NullLogger<PriceApiClient>());

            var subscription = client.GetSubscriptionResource(new ResourceIdentifier($"/subscriptions/{configuration.SubscriptionId}"));
            var batchAccount =
                ((await (await subscription.GetBatchAccountsAsync(cancellationToken: cancellationTokenSource.Token).SingleOrDefaultAsync(r =>
                    configuration.BatchAccount.Equals(r.Id.Name, StringComparison.OrdinalIgnoreCase),
                    cancellationToken: cancellationTokenSource.Token))?.GetAsync(cancellationToken: cancellationTokenSource.Token)!)?.Value)
                ?? throw new Exception($"Batch account {configuration.BatchAccount} not found.");

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

            using var batchClient = Microsoft.Azure.Batch.BatchClient.Open(new Microsoft.Azure.Batch.Auth.BatchTokenCredentials(
                new UriBuilder(Uri.UriSchemeHttps, batchAccount.Data.AccountEndpoint).Uri.AbsoluteUri,
                async () => await BatchTokenProvider(tokenCredential, cancellationTokenSource.Token)));

            Console.WriteLine("Testing each SKU...");
            ConcurrentBag<VirtualMachineInformation> vms = new();
            count = batchSupportedVmSet.Count;
            index = 0;

            await Parallel.ForEachAsync(batchSupportedVmSet, new ParallelOptions { MaxDegreeOfParallelism = 3, CancellationToken = cancellationTokenSource.Token }, async (name, token) =>
                await GetVirtualMachineInformationsAsync(name, batchClient, batchAccount.Data).ForEachAsync(vm => vms.Add(vm), token));

            var batchVmInfo = vms
                .OrderBy(x => x!.VmSize).ThenBy(x => x!.LowPriority)
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

        private int index;
        private int count;

        private async IAsyncEnumerable<VirtualMachineInformation> GetVirtualMachineInformationsAsync(string name, Microsoft.Azure.Batch.BatchClient batchClient, BatchAccountData batchAccount)
        {
            if (name is null)
            {
                yield break;
            }

            Console.Write($"Testing {Interlocked.Increment(ref index)} out of {count}...\r");

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

            if (regionsForVm[name].Contains(batchAccount.Location!.Value.Name))
            {
                var pool = batchClient.PoolOperations.CreatePool(sku.Name, sizeInfo.Name, GetMachineConfiguration(generations.Contains("V2"), encryptionAtHostSupported), targetDedicatedComputeNodes: lowPriorityCapable ? 0 : 1, targetLowPriorityComputeNodes: lowPriorityCapable ? 1 : 0);
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
                        Console.WriteLine($"Skipping {name} because the batch account failed to allocate nodes into a pool due to '{string.Join("', '", pool.ResizeErrors.Select(e => e.Code))}'.");
                        Console.WriteLine($"    Additional information: Message: '{string.Join("', '", pool.ResizeErrors.Select(e => e.Message))}'{Environment.NewLine}    Values: '{string.Join("', '", pool.ResizeErrors.SelectMany(e => e.Values ?? new List<Microsoft.Azure.Batch.NameValuePair>()).Select(new Func<Microsoft.Azure.Batch.NameValuePair, string>(detail => $"\"{detail.Name}\": \"{detail.Value}\"")))}'.");
                        yield break;
                    }

                    List<Microsoft.Azure.Batch.ComputeNode> nodes;

                    do
                    {
                        await Task.Delay(TimeSpan.FromSeconds(5), cancellationTokenSource.Token);
                        nodes = await pool.ListComputeNodes(detailLevel: new Microsoft.Azure.Batch.ODATADetailLevel(selectClause: "id,state,errors")).ToAsyncEnumerable().ToListAsync(cancellationTokenSource.Token);

                        if (nodes.Any(n => n.Errors?.Any() ?? false))
                        {
                            Console.WriteLine($"Skipping {name} because nodes failed to start in our batch account's pool due to '{string.Join("', '", nodes.SelectMany(n => n.Errors).Select(e => e.Code))}'.");
                            yield break;
                        }

                        if (nodes.Any(n => Microsoft.Azure.Batch.Common.ComputeNodeState.Unusable == n.State))
                        {
                            Console.WriteLine($"Skipping {name} because nodes became unusable without ever being ready in our batch account's pool.");
                            yield break;
                        }
                    }
                    while (nodes.Any(n => Microsoft.Azure.Batch.Common.ComputeNodeState.Idle != n.State));
                }
                finally
                {
                    try
                    {
                        await pool.DeleteAsync(cancellationToken: CancellationToken.None);
                    }
                    catch (Microsoft.Azure.Batch.Common.BatchException exception)
                    {
                        switch (exception.RequestInformation.HttpStatusCode)
                        {
                            case System.Net.HttpStatusCode.Conflict:
                                break;

                            default:
                                Console.WriteLine($"Failed to delete '{name}' pool: {exception.Message}");
                                break;
                        }
                    }
                    catch (InvalidOperationException exception)
                    {
                        Console.WriteLine($"Failed to delete '{name}' pool: {exception.Message}");
                    }
                }
            }
            else
            {
                Console.WriteLine($"Skipping {name} because it is not available in this batch account's region.");
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
                PricePerHour = priceForVm.TryGetValue(name, out var priceItem) ? (decimal?)priceItem.retailPrice : null,
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
                    PricePerHour = lowPrPriceForVm.TryGetValue(name, out var lowPrPriceItem) ? (decimal?)lowPrPriceItem.retailPrice : null,
                };
            }
        }

        private static Microsoft.Azure.Batch.VirtualMachineConfiguration GetMachineConfiguration(bool useV2, bool encryptionAtHostSupported)
        {
            return new(useV2 ? V2ImageReference : V1ImageReference, "batch.node.ubuntu 20.04")
            {
                DiskEncryptionConfiguration = encryptionAtHostSupported ? new Microsoft.Azure.Batch.DiskEncryptionConfiguration(targets: new List<Microsoft.Azure.Batch.Common.DiskEncryptionTarget> () { Microsoft.Azure.Batch.Common.DiskEncryptionTarget.OsDisk, Microsoft.Azure.Batch.Common.DiskEncryptionTarget.TemporaryDisk }) : null
            };
        }

        private static Microsoft.Azure.Batch.ImageReference V1ImageReference
            = new("ubuntu-server-container", "microsoft-azure-batch", "20-04-lts", "latest");

        private static Microsoft.Azure.Batch.ImageReference V2ImageReference
            = new("ubuntu-hpc", "microsoft-dsvm", "2004", "latest");
    }
}
