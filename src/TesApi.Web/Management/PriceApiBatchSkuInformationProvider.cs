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
using Tes.ApiClients.Models.Pricing;
using Tes.Models;
using BatchModels = Azure.ResourceManager.Batch.Models;

namespace TesApi.Web.Management
{
    /// <summary>
    /// Provides pricing and size VM information using the retail pricing API. 
    /// </summary>
    public class PriceApiBatchSkuInformationProvider : IBatchSkuInformationProvider
    {
        internal const int StartingLun = 0;
        private readonly PriceApiClient priceApiClient;
        private readonly IMemoryCache appCache;
        private readonly AzureCloudConfig azureCloudConfig;
        private readonly bool IsTerraConfigured;
        private readonly ILogger logger;

        private static object VmSizesAndPricesKey(string region) => $"VmSizesAndPrices-{region}";
        private static object StorageDisksAndPricesKey(string region) => $"StorageDisksAndPrices-{region}";
        private static object StorageDisksAndPricesKey(string region, double capacity, int maxDataDiskCount) => $"StorageDisksAndPrices-{region}-{capacity.ToString(System.Globalization.CultureInfo.InvariantCulture)}-{maxDataDiskCount.ToString(System.Globalization.CultureInfo.InvariantCulture)}";

        /// <summary>
        /// Converts <see cref="StorageDiskPriceInformation"/> to <see cref="VmDataDisks"/>.
        /// </summary>
        /// <param name="disk"><see cref="StorageDiskPriceInformation"/>.</param>
        /// <param name="lun">lun.</param>
        /// <param name="storageAccountType">Type of the storage account.</param>
        /// <param name="caching">The caching.</param>
        /// <returns></returns>
        public static VmDataDisks ToVmDataDisk(StorageDiskPriceInformation disk, int lun, BatchModels.BatchStorageAccountType storageAccountType, BatchModels.BatchDiskCachingType? caching) => new(disk.PricePerHour, lun, disk.CapacityInGiB, caching?.ToString(), storageAccountType.ToString());

        /// <summary>
        /// Constructor of PriceApiBatchSkuInformationProvider
        /// </summary>
        /// <param name="appCache">Cache instance. If null, calls to the retail pricing API won't be cached.</param>
        /// <param name="priceApiClient">Retail pricing API client.</param>
        /// <param name="azureCloudConfig"></param>
        /// <param name="isTerraConfigured"></param>
        /// <param name="logger">Logger instance. </param>
        public PriceApiBatchSkuInformationProvider(IMemoryCache appCache, PriceApiClient priceApiClient, AzureCloudConfig azureCloudConfig,
            bool isTerraConfigured, ILogger<PriceApiBatchSkuInformationProvider> logger)
        {
            ArgumentNullException.ThrowIfNull(priceApiClient);
            ArgumentNullException.ThrowIfNull(azureCloudConfig);
            ArgumentNullException.ThrowIfNull(logger);

            this.appCache = appCache;
            this.priceApiClient = priceApiClient;
            this.azureCloudConfig = azureCloudConfig;
            IsTerraConfigured = isTerraConfigured;
            this.logger = logger;
        }

        /// <summary>
        /// Constructor of PriceApiBatchSkuInformationProvider.
        /// </summary>
        /// <param name="priceApiClient">Retail pricing API client.</param>
        /// <param name="azureCloudConfig"></param>
        /// <param name="logger">Logger instance.</param>
        internal PriceApiBatchSkuInformationProvider(PriceApiClient priceApiClient, AzureCloudConfig azureCloudConfig,
            ILogger<PriceApiBatchSkuInformationProvider> logger)
            : this(null, priceApiClient, azureCloudConfig, false, logger)
        { }

        /// <inheritdoc />
        public async Task<List<VirtualMachineInformation>> GetVmSizesAndPricesAsync(string region, CancellationToken cancellationToken)
        {
            if (appCache is null)
            {
                return await GetVmSizesAndPricesImplAsync(region, cancellationToken);
            }

            logger.LogInformation("Trying to get pricing information from the cache for region: {Region}.", region);

            return await appCache.GetOrCreateAsync(VmSizesAndPricesKey(region), async _1 => await GetVmSizesAndPricesImplAsync(region, cancellationToken));
        }

        /// <inheritdoc />
        public async Task<List<VmDataDisks>> GetStorageDisksAndPricesAsync(string region, double capacity, int maxDataDiskCount, CancellationToken cancellationToken)
        {
            if (IsTerraConfigured) // Terra does not support adding additional data disks
            {
                return [];
            }

            if (appCache is null)
            {
                return await GetStorageDisksAndPricesImplAsync(region, capacity, maxDataDiskCount, cancellationToken);
            }

            logger.LogInformation("Trying to get pricing information from the cache for region: {Region}.", region);

            return await appCache.GetOrCreateAsync(StorageDisksAndPricesKey(region, capacity, maxDataDiskCount), async _1 => await GetStorageDisksAndPricesImplAsync(region, capacity, maxDataDiskCount, cancellationToken));
        }

        private async Task<List<VirtualMachineInformation>> GetVmSizesAndPricesImplAsync(string region, CancellationToken cancellationToken)
        {
            logger.LogInformation("Getting VM sizes and price information for region: {Region}", region);

            var localVmSizeInfoForBatchSupportedSkus = (await GetLocalDataAsync<VirtualMachineInformation>($"BatchSupportedVmSizeInformation_{azureCloudConfig.Name.ToUpperInvariant()}.json", "Reading local VM size information from file: {LocalVmPriceList}", cancellationToken))
                .Where(x => x.RegionsAvailable.Contains(region, StringComparer.OrdinalIgnoreCase))
                .ToList();

            logger.LogInformation("localVmSizeInfoForBatchSupportedSkus.Count: {CountOfPrepreparedSkuRecordsInRegion}", localVmSizeInfoForBatchSupportedSkus.Count);

            try
            {
                var pricingItems = await priceApiClient.GetAllPricingInformationForNonWindowsAndNonSpotVmsAsync(region, cancellationToken).ToListAsync(cancellationToken);

                logger.LogInformation("Received {CountOfSkuPrice} pricing items", pricingItems.Count);

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
                    "Returning {CountOfSupportedVmSkus} Vm information entries with pricing for Azure Batch Supported Vm types.", vmInfoList.Count);

                return vmInfoList;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex,
                    $"Exception encountered retrieving live pricing data, reverting to local pricing data.");
                return new(localVmSizeInfoForBatchSupportedSkus.Select(sku => { sku.RegionsAvailable = []; return sku; }));
            }
        }

        private static VirtualMachineInformation CreateVirtualMachineInfoFromReference(
            VirtualMachineInformation vmReference, bool isLowPriority, decimal pricePerHour)
        {
            var result = VirtualMachineInformation.Clone(vmReference);
            result.LowPriority = isLowPriority;
            result.PricePerHour = pricePerHour;
            result.RegionsAvailable = []; // Not used by TES outside of this method & this array inflates object sizes in task returns and task repository needlessly.
            return result;
        }

        private async Task<List<VmDataDisks>> GetStorageDisksAndPricesImplAsync(string region, double capacity, int maxDataDiskCount, CancellationToken cancellationToken)
        {
            logger.LogInformation("Getting VM sizes and price information for region: {Region}", region);

            var localStorageDisksAndPrices = (await GetLocalDataAsync<StorageDiskPriceInformation>("BatchDataDiskInformation.json", "Reading local storage disk information from file: {LocalStoragePriceList}", cancellationToken)).ToList();

            logger.LogInformation("localStorageDisksAndPrices.Count: {CountOfLocalStorageDisks}", localStorageDisksAndPrices.Count);

            try
            {
                var pricingItems = await (appCache?.GetOrCreateAsync(StorageDisksAndPricesKey(region), async _1 => await GetPricingData(priceApiClient, region, cancellationToken)) ?? GetPricingData(priceApiClient, region, cancellationToken));
                logger.LogInformation("Received {CountOfDataDiskPrice} pricing items", pricingItems.Count);

                if (pricingItems.Count == 0)
                {
                    logger.LogWarning("No pricing information received from the retail pricing API. Reverting to local pricing data.");
                    return DetermineDisks(localStorageDisksAndPrices, capacity, maxDataDiskCount);
                }

                return DetermineDisks(pricingItems.Select(item => new StorageDiskPriceInformation(name: item.meterName, capacity: StorageDiskPriceInformation.StandardLrsSsdCapacityInGiB[item.meterName], price: Convert.ToDecimal(item.unitPrice))), capacity, maxDataDiskCount);

                async static Task<List<PricingItem>> GetPricingData(PriceApiClient priceApiClient, string region, CancellationToken cancellationToken)
                {
                    return await priceApiClient.GetAllPricingInformationForStandardStorageLRSDisksAsync(region, cancellationToken)
                        .Where(item => StorageDiskPriceInformation.StandardLrsSsdCapacityInGiB.ContainsKey(item.meterName))
                        .ToListAsync(cancellationToken);
                }
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex,
                    $"Exception encountered retrieving live pricing data, reverting to local pricing data.");
                return DetermineDisks(localStorageDisksAndPrices, capacity, maxDataDiskCount);
            }
        }

        internal static List<VmDataDisks> DetermineDisks(IEnumerable<StorageDiskPriceInformation> diskPriceInformation, double capacity, int maxDataDiskCount)
        {
            if (maxDataDiskCount <= 0)
            {
                return []; // No disks can be added
            }

            var diskPricesBySize = diskPriceInformation.ToDictionary(disk => disk.CapacityInGiB);

            if (FindSizeMinimumGreaterOrEqualIfExistsOrMaximum(diskPricesBySize.Keys, capacity) * maxDataDiskCount < capacity)
            {
                return []; // Sufficient total capacity cannot be added
            }

            var perDiskCapacity = FindSizeMinimumGreaterOrEqualIfExistsOrMaximum(diskPricesBySize.Keys, capacity / maxDataDiskCount);
            return Enumerable.Repeat(diskPricesBySize[perDiskCapacity], (int)Math.Round(capacity / perDiskCapacity, MidpointRounding.ToPositiveInfinity))
                .Select((disk, i) => ToVmDataDisk(disk, StartingLun + i, BatchModels.BatchStorageAccountType.StandardSsdLrs, BatchModels.BatchDiskCachingType.ReadOnly))
                .ToList();

            static int FindSizeMinimumGreaterOrEqualIfExistsOrMaximum(IEnumerable<int> availableSizes, double request)
            {
                try
                {
                    return availableSizes.Where(size => size >= request).Min();
                }
                catch (InvalidOperationException)
                {
                    return availableSizes.Max();
                }
            }
        }

        private async Task<List<T>> GetLocalDataAsync<T>(string fileName, string logMessage, CancellationToken cancellationToken)
        {
            var filePath = Path.Combine(AppContext.BaseDirectory, fileName);
#pragma warning disable CA2254 // Template should be a static expression
            logger.LogInformation(logMessage, filePath);
#pragma warning restore CA2254 // Template should be a static expression

            return JsonConvert.DeserializeObject<List<T>>(
                await File.ReadAllTextAsync(filePath, cancellationToken));
        }
    }
}
