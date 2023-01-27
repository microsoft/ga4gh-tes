// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using LazyCache;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Tes.Models;
using TesApi.Web.Management.Clients;

namespace TesApi.Web.Management
{
    /// <summary>
    /// Provides pricing and size VM information using the retail pricing API. 
    /// </summary>
    public class PriceApiBatchSkuInformationProvider : IBatchSkuInformationProvider
    {
        private readonly PriceApiClient priceApiClient;
        private readonly IAppCache appCache;
        private readonly ILogger logger;

        /// <summary>
        /// Constructor of PriceApiBatchSkuInformationProvider
        /// </summary>
        /// <param name="appCache">Cache instance. If null, calls to the retail pricing API won't be cached.</param>
        /// <param name="priceApiClient">Retail pricing API client.</param>
        /// <param name="logger">Logger instance. </param>
        public PriceApiBatchSkuInformationProvider(IAppCache appCache, PriceApiClient priceApiClient,
            ILogger<PriceApiBatchSkuInformationProvider> logger)
        {
            ArgumentNullException.ThrowIfNull(priceApiClient);
            ArgumentNullException.ThrowIfNull(logger);

            this.appCache = appCache;
            this.priceApiClient = priceApiClient;
            this.logger = logger;
        }

        /// <summary>
        /// Constructor of PriceApiBatchSkuInformationProvider.
        /// </summary>
        /// <param name="priceApiClient">Retail pricing API client.</param>
        /// <param name="logger">Logger instance.</param>
        public PriceApiBatchSkuInformationProvider(PriceApiClient priceApiClient,
            ILogger<PriceApiBatchSkuInformationProvider> logger)
            : this(null, priceApiClient, logger)
        {
        }

        /// <inheritdoc />
        public async Task<List<VirtualMachineInformation>> GetVmSizesAndPricesAsync(string region)
        {
            if (appCache is null)
            {
                return await GetVmSizesAndPricesAsyncImpl(region);
            }

            logger.LogInformation($"Trying to get pricing information from the cache for region: {region}.");

            return await appCache.GetOrAddAsync(region, async () => await GetVmSizesAndPricesAsyncImpl(region));
        }

        private async Task<List<VirtualMachineInformation>> GetVmSizesAndPricesAsyncImpl(string region)
        {
            logger.LogInformation($"Getting VM sizes and price information for region:{region}");

            var localVmSizeInfoForBatchSupportedSkus = (await GetLocalVmSizeInformationForBatchSupportedSkusAsync()).Where(x => x.RegionsAvailable.Contains(region, StringComparer.OrdinalIgnoreCase));
            var pricingItems = await priceApiClient.GetAllPricingInformationForNonWindowsAndNonSpotVmsAsync(region)
                .ToListAsync();

            logger.LogInformation($"Received {pricingItems.Count} pricing items");

            var vmInfoList = new List<VirtualMachineInformation>();

            foreach (var vm in localVmSizeInfoForBatchSupportedSkus)
            {

                var instancePricingInfo = pricingItems.Where(p => p.armSkuName == vm.VmSize).ToList();
                var normalPriorityInfo = instancePricingInfo.FirstOrDefault(s =>
                    s.skuName.Contains(" Low Priority", StringComparison.OrdinalIgnoreCase));
                var lowPriorityInfo = instancePricingInfo.FirstOrDefault(s =>
                    !s.skuName.Contains(" Low Priority", StringComparison.OrdinalIgnoreCase));

                if (lowPriorityInfo is not null)
                {
                    vmInfoList.Add(CreateVirtualMachineInfoFromReference(vm, true,
                        Convert.ToDecimal(lowPriorityInfo.unitPrice)));
                }

                if (normalPriorityInfo is not null)
                {
                    vmInfoList.Add(CreateVirtualMachineInfoFromReference(vm, false,
                        Convert.ToDecimal(normalPriorityInfo.unitPrice)));
                }
            }

            logger.LogInformation(
                $"Returning {vmInfoList.Count} Vm information entries with pricing for Azure Batch Supported Vm types");

            return vmInfoList;

        }

        private static VirtualMachineInformation CreateVirtualMachineInfoFromReference(
            VirtualMachineInformation vmReference, bool isLowPriority, decimal pricePerHour)
            => new()
            {
                LowPriority = isLowPriority,
                MaxDataDiskCount = vmReference.MaxDataDiskCount,
                MemoryInGB = vmReference.MemoryInGB,
                NumberOfCores = vmReference.NumberOfCores,
                PricePerHour = pricePerHour,
                ResourceDiskSizeInGB = vmReference.ResourceDiskSizeInGB,
                VmFamily = vmReference.VmFamily,
                VmSize = vmReference.VmSize,
                RegionsAvailable = vmReference.RegionsAvailable,
                HyperVGenerations = vmReference.HyperVGenerations
            };

        private static async Task<List<VirtualMachineInformation>> GetLocalVmSizeInformationForBatchSupportedSkusAsync()
        {
            return JsonConvert.DeserializeObject<List<VirtualMachineInformation>>(
                await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory,
                    "BatchSupportedVmSizeInformation.json")));
        }
    }
}
