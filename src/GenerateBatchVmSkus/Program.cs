// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Tes.Models;

namespace TesUtils
{
    internal class Configuration
    {
        public string? SubscriptionId { get; set; }
        public string? OutputFilePath { get; set; }
        public string? TestedVmSkus { get; set; }

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

            Environment.Exit(await RunAsync(configuration));
        }

        static async Task<int> RunAsync(Configuration configuration)
        {
            ArgumentException.ThrowIfNullOrEmpty(configuration.OutputFilePath);
            ArgumentException.ThrowIfNullOrEmpty(configuration.SubscriptionId);
            ArgumentException.ThrowIfNullOrEmpty(configuration.TestedVmSkus);

            var vmPrices = JsonConvert.DeserializeObject<IEnumerable<VmPrice>>(File.ReadAllText(configuration.TestedVmSkus));
            if (vmPrices is null)
            {
                throw new Exception($"Error parsing {configuration.TestedVmSkus}");
            }

            var validSet = vmPrices.Select(vm => vm.VmSize).ToHashSet();

            var client = new ArmClient(new DefaultAzureCredential());
            static double ConvertMiBToGiB(int value) => Math.Round(value / 1024.0, 2);
            var subscription = client.GetSubscriptionResource(new ResourceIdentifier($"/subscriptions/{configuration.SubscriptionId}"));

            var regionsForVm = new Dictionary<string, HashSet<string>>();
            var sizeForVm = new Dictionary<string, VirtualMachineSize>();
            var skuForVm = new Dictionary<string, ComputeResourceSku>();

            var regions = await subscription.GetLocationsAsync().Where(x => x.Metadata.RegionType == RegionType.Physical).ToListAsync();

            foreach (var region in regions)
            {
                try
                {
                    var location = new AzureLocation(region.Name);
                    var vms = await subscription.GetBatchSupportedVirtualMachineSkusAsync(location).Select(s => s.Name).ToListAsync();

                    List<VirtualMachineSize>? sizes = null;
                    List<ComputeResourceSku>? skus = null;

                    foreach (var vm in vms)
                    {
                        if (regionsForVm.TryGetValue(vm, out var value))
                        {
                            value.Add(region.Name);
                        }
                        else
                        {
                            regionsForVm[vm] = new() { region.Name };
                        }

                        if (!sizeForVm.ContainsKey(vm))
                        {
                            sizes ??= await subscription.GetVirtualMachineSizesAsync(location).ToListAsync();
                            sizeForVm[vm] = sizes.Single(vmsize => vmsize.Name.Equals(vm, StringComparison.OrdinalIgnoreCase));
                        }

                        if (!skuForVm.ContainsKey(vm))
                        {
                            skus ??= await subscription.GetComputeResourceSkusAsync($"location eq '{region.Name}'").ToListAsync();
                            skuForVm[vm] = skus.Single(sku => sku.Name.Equals(vm, StringComparison.OrdinalIgnoreCase));
                        }
                    }

                    Console.WriteLine($"{region.Name} supportedSkuCount:{vms.Count}");
                }
                catch (RequestFailedException e)
                {
                    Console.WriteLine($"No skus supported in {region.Name}. {e.ErrorCode}");
                }
            }

            var batchSupportedVmSet = regionsForVm.Keys.ToList();
            Console.WriteLine($"Superset supportedSkuCount:{batchSupportedVmSet.Count}");

            var batchVmInfo = batchSupportedVmSet.Select((s) =>
            {
                if (!validSet.Contains(s))
                {
                    Console.WriteLine($"Skipping {s} not in valid vm skus file.");
                    return null;
                }

                var sizeInfo = sizeForVm[s];
                var sku = skuForVm[s];

                if (sizeInfo is null || sizeInfo.MemoryInMB is null || sizeInfo.ResourceDiskSizeInMB is null)
                {
                    throw new Exception($"Size info is null for VM {s}");
                }

                if (sku is null)
                {
                    throw new Exception($"Sku info is null for VM {s}");
                }

                var generationList = new List<string>();
                var generation = sku?.Capabilities.Where(x => x.Name.Equals("HyperVGenerations")).SingleOrDefault()?.Value;

                if (generation is not null)
                {
                    generationList = generation.Split(",").ToList();
                }

                _ = int.TryParse(sku?.Capabilities.Where(x => x.Name.Equals("vCPUsAvailable")).SingleOrDefault()?.Value, out var vCpusAvailable);

                return new VirtualMachineInformation()
                {
                    MaxDataDiskCount = sizeInfo.MaxDataDiskCount,
                    MemoryInGiB = ConvertMiBToGiB(sizeInfo.MemoryInMB!.Value),
                    VCpusAvailable = vCpusAvailable,
                    ResourceDiskSizeInGiB = ConvertMiBToGiB(sizeInfo.ResourceDiskSizeInMB!.Value),
                    VmSize = sizeInfo.Name,
                    VmFamily = sku?.Family,
                    HyperVGenerations = generationList,
                    RegionsAvailable = new List<string>(regionsForVm[s].Order())
                };
            }).Where(x => x is not null).OrderBy(x => x!.VmSize).ToList();

            var jsonOptions = new JsonSerializerOptions(JsonSerializerDefaults.General)
            {
                WriteIndented = true
            };
            var data = System.Text.Json.JsonSerializer.Serialize(batchVmInfo, options: jsonOptions);
            await File.WriteAllTextAsync(configuration.OutputFilePath!, data);
            return 0;
        }
    }

    public class VmPrice
    {
#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
        public string VmSize { get; set; }
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
        public decimal? PricePerHourDedicated { get; set; }
        public decimal? PricePerHourLowPriority { get; set; }

        [JsonIgnore]
        public bool LowPriorityAvailable => PricePerHourLowPriority is not null;
    }
}
