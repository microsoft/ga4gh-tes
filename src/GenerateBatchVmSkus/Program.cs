// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
using Azure.Core;
using Azure.Identity;
using Azure.ResourceManager;
using Azure.ResourceManager.Batch;
using Microsoft.Extensions.Configuration;
using System.Reflection;
using System.Text.Json;
using Tes.Models;
using Azure.ResourceManager.Compute;
using Azure;
using Azure.ResourceManager.Compute.Models;
using Azure.ResourceManager.Resources.Models;

namespace TesUtils
{
    internal class Configuration
    {
        public string? SubscriptionId { get; set; }
        public string? OutputFilePath { get; set; }

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
                        if (regionsForVm.ContainsKey(vm))
                        {
                            regionsForVm[vm].Add(region.Name);
                        }
                        else
                        {
                            regionsForVm[vm] = new HashSet<string>() { region.Name };
                        }

                        if (!sizeForVm.ContainsKey(vm))
                        {
                            if (sizes is null)
                            {
                                sizes = await subscription.GetVirtualMachineSizesAsync(location).ToListAsync();
                            }
                            sizeForVm[vm] = sizes.SingleOrDefault(vmsize => vmsize.Name.Equals(vm, StringComparison.OrdinalIgnoreCase));
                        }

                        if (!skuForVm.ContainsKey(vm))
                        {
                            if (skus is null)
                            {
                                skus = await subscription.GetComputeResourceSkusAsync($"location eq '{region.Name}'").ToListAsync();
                            }
                            skuForVm[vm] = skus.SingleOrDefault(sku => sku.Name.Equals(vm, StringComparison.OrdinalIgnoreCase));
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
                var sizeInfo = sizeForVm[s];
                var sku = skuForVm[s];

                if (sizeInfo is null)
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

                int.TryParse(sku?.Capabilities.Where(x => x.Name.Equals("vCPUsAvailable")).SingleOrDefault()?.Value, out var vCpusAvailable);

                return new VirtualMachineInformation()
                {
                    MaxDataDiskCount = sizeInfo.MaxDataDiskCount,
                    MemoryInGiB = ConvertMiBToGiB(sizeInfo.MemoryInMB ?? 0),
                    VCpusAvailable = vCpusAvailable,
                    ResourceDiskSizeInGiB = ConvertMiBToGiB(sizeInfo.ResourceDiskSizeInMB ?? 0),
                    VmSize = sizeInfo.Name,
                    VmFamily = sku?.Family,
                    HyperVGenerations = generationList,
                    RegionsAvailable = new List<string>(regionsForVm[s])
                };
            }).OrderBy(x => x.VmSize).ToList();

            var jsonOptions = new JsonSerializerOptions(JsonSerializerDefaults.General);
            jsonOptions.WriteIndented = true;
            var data = JsonSerializer.Serialize(batchVmInfo, options: jsonOptions);
            await File.WriteAllTextAsync(configuration.OutputFilePath, data);
            return 0;
        }
    }
}
