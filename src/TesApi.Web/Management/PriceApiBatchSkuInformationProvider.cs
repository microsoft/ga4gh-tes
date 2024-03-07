// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CommonUtilities.AzureCloud;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Tes.ApiClients;
using Tes.Models;

namespace TesApi.Web.Management
{
    /// <summary>
    /// Provides pricing and size VM information using the retail pricing API. 
    /// </summary>
    public class PriceApiBatchSkuInformationProvider : IBatchSkuInformationProvider
    {
        private readonly PriceApiClient priceApiClient;
        private readonly IMemoryCache appCache;
        private readonly AzureCloudConfig azureCloudConfig;
        private readonly ILogger logger;

        /// <summary>
        /// Constructor of PriceApiBatchSkuInformationProvider
        /// </summary>
        /// <param name="appCache">Cache instance. If null, calls to the retail pricing API won't be cached.</param>
        /// <param name="priceApiClient">Retail pricing API client.</param>
        /// <param name="azureCloudConfig"></param>
        /// <param name="logger">Logger instance. </param>
        public PriceApiBatchSkuInformationProvider(IMemoryCache appCache, PriceApiClient priceApiClient, AzureCloudConfig azureCloudConfig,
            ILogger<PriceApiBatchSkuInformationProvider> logger)
        {
            ArgumentNullException.ThrowIfNull(priceApiClient);
            ArgumentNullException.ThrowIfNull(azureCloudConfig);
            ArgumentNullException.ThrowIfNull(logger);

            this.appCache = appCache;
            this.priceApiClient = priceApiClient;
            this.azureCloudConfig = azureCloudConfig;
            this.logger = logger;
        }

        /// <summary>
        /// Constructor of PriceApiBatchSkuInformationProvider.
        /// </summary>
        /// <param name="priceApiClient">Retail pricing API client.</param>
        /// <param name="azureCloudConfig"></param>
        /// <param name="logger">Logger instance.</param>
        public PriceApiBatchSkuInformationProvider(PriceApiClient priceApiClient, AzureCloudConfig azureCloudConfig,
            ILogger<PriceApiBatchSkuInformationProvider> logger)
            : this(null, priceApiClient, azureCloudConfig, logger)
        {
        }

        /// <inheritdoc />
        public async Task<List<VirtualMachineInformation>> GetVmSizesAndPricesAsync(string region, CancellationToken cancellationToken)
        {
            if (appCache is null)
            {
                return await GetVmSizesAndPricesAsyncImpl(region, cancellationToken);
            }

            logger.LogInformation($"Trying to get pricing information from the cache for region: {region}.");

            return await appCache.GetOrCreateAsync(region, async _1 => await GetVmSizesAndPricesAsyncImpl(region, cancellationToken));
        }

        private async Task<List<VirtualMachineInformation>> GetVmSizesAndPricesAsyncImpl(string region, CancellationToken cancellationToken)
        {
            logger.LogInformation($"Getting VM sizes and price information for region: {region}");

            var localVmSizeInfoForBatchSupportedSkus = (await GetLocalVmSizeInformationForBatchSupportedSkusAsync(cancellationToken)).Where(x => x.RegionsAvailable.Contains(region, StringComparer.OrdinalIgnoreCase)).ToList();

            logger.LogInformation($"localVmSizeInfoForBatchSupportedSkus.Count: {localVmSizeInfoForBatchSupportedSkus.Count}");

            try
            {
                var pricingItems = await priceApiClient.GetAllPricingInformationForNonWindowsAndNonSpotVmsAsync(region, cancellationToken).ToListAsync(cancellationToken);

                logger.LogInformation($"Received {pricingItems.Count} pricing items");

                if (pricingItems == null || pricingItems.Count == 0)
                {
                    logger.LogWarning("No pricing information received from the retail pricing API. Reverting to local pricing data.");
                    return new List<VirtualMachineInformation>(localVmSizeInfoForBatchSupportedSkus);
                }

                var vmInfoList = new List<VirtualMachineInformation>();

                foreach (var vm in localVmSizeInfoForBatchSupportedSkus.Where(v => !v.LowPriority))
                {

                    var instancePricingInfo = pricingItems.Where(p => p.armSkuName == vm.VmSize && p.effectiveStartDate < DateTime.UtcNow).ToList();
                    var normalPriorityInfo = instancePricingInfo.Where(s =>
                        !s.skuName.Contains(" Low Priority", StringComparison.OrdinalIgnoreCase)).MaxBy(p => p.effectiveStartDate);
                    var lowPriorityInfo = instancePricingInfo.Where(s =>
                        s.skuName.Contains(" Low Priority", StringComparison.OrdinalIgnoreCase)).MaxBy(p => p.effectiveStartDate);

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
            catch (Exception ex)
            {
                logger.LogWarning(ex,
                    $"Exception encountered retrieving live pricing data, reverting to local pricing data.");
                return new List<VirtualMachineInformation>(localVmSizeInfoForBatchSupportedSkus);
            }
        }

        private static VirtualMachineInformation CreateVirtualMachineInfoFromReference(
            VirtualMachineInformation vmReference, bool isLowPriority, decimal pricePerHour)
            => new()
            {
                LowPriority = isLowPriority,
                MaxDataDiskCount = vmReference.MaxDataDiskCount,
                MemoryInGiB = vmReference.MemoryInGiB,
                VCpusAvailable = vmReference.VCpusAvailable,
                PricePerHour = pricePerHour,
                ResourceDiskSizeInGiB = vmReference.ResourceDiskSizeInGiB,
                VmFamily = vmReference.VmFamily,
                VmSize = vmReference.VmSize,
                RegionsAvailable = vmReference.RegionsAvailable,
                HyperVGenerations = vmReference.HyperVGenerations,
                EncryptionAtHostSupported = vmReference.EncryptionAtHostSupported
            };

        private async Task<List<VirtualMachineInformation>> GetLocalVmSizeInformationForBatchSupportedSkusAsync(CancellationToken cancellationToken)
        {
            string filePath = Path.Combine(AppContext.BaseDirectory, $"BatchSupportedVmSizeInformation_{azureCloudConfig.Name.ToUpperInvariant()}.json");
            logger.LogInformation("Reading local VM size information from file: {0}", filePath);

            return JsonConvert.DeserializeObject<List<VirtualMachineInformation>>(
                await File.ReadAllTextAsync(filePath, cancellationToken));
        }
    }
}
