// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Tes.Models;
using TesApi.Web.Management;
using TesApi.Web.Management.Models.Quotas;
using TesApi.Web.Options;
using TesApi.Web.Storage;

namespace TesApi.Web
{
    /// <summary>
    /// Provides methods for handling the configuration files in the configuration container
    /// </summary>
    public class ConfigurationUtils
    {
        private readonly string defaultStorageAccountName;
        private readonly IStorageAccessProvider storageAccessProvider;
        private readonly ILogger<ConfigurationUtils> logger;
        private readonly IBatchQuotaProvider quotaProvider;
        private readonly IBatchSkuInformationProvider skuInformationProvider;
        private readonly BatchAccountResourceInformation batchAccountResourceInformation;

        /// <summary>
        /// The constructor
        /// </summary>
        /// <param name="defaultStorageOptions">Configuration of <see cref="StorageOptions"/></param>
        /// <param name="storageAccessProvider"><see cref="IStorageAccessProvider"/></param>
        /// <param name="quotaProvider"><see cref="IBatchQuotaProvider"/>></param>
        /// <param name="skuInformationProvider"><see cref="IBatchSkuInformationProvider"/>></param>
        /// <param name="batchAccountResourceInformation"><see cref="BatchAccountResourceInformation"/></param>
        /// <param name="logger"><see cref="ILogger"/></param>
        public ConfigurationUtils(
            IOptions<Options.StorageOptions> defaultStorageOptions,
            IStorageAccessProvider storageAccessProvider,
            IBatchQuotaProvider quotaProvider,
            IBatchSkuInformationProvider skuInformationProvider,
            BatchAccountResourceInformation batchAccountResourceInformation,
            ILogger<ConfigurationUtils> logger)
        {
            ArgumentNullException.ThrowIfNull(storageAccessProvider);
            ArgumentNullException.ThrowIfNull(quotaProvider);
            ArgumentNullException.ThrowIfNull(batchAccountResourceInformation);
            if (string.IsNullOrEmpty(batchAccountResourceInformation.Region))
            {
                throw new ArgumentException(
                    $"The batch information provided does not include region. Batch information:{batchAccountResourceInformation}");
            }
            ArgumentNullException.ThrowIfNull(logger);

            this.defaultStorageAccountName = defaultStorageOptions.Value.DefaultAccountName;
            this.storageAccessProvider = storageAccessProvider;
            this.logger = logger;
            this.quotaProvider = quotaProvider;
            this.skuInformationProvider = skuInformationProvider;
            this.batchAccountResourceInformation = batchAccountResourceInformation;
        }

        /// <summary>
        /// Combines the allowed-vm-sizes configuration file and list of supported+available VMs to produce the supported-vm-sizes file and tag incorrect 
        /// entries in the allowed-vm-sizes file with a warning. Sets the AllowedVmSizes configuration key.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public async Task<List<string>> ProcessAllowedVmSizesConfigurationFileAsync(CancellationToken cancellationToken)
        {
            var supportedVmSizesUrl = new Uri(await storageAccessProvider.GetInternalTesBlobUrlAsync("/configuration/supported-vm-sizes", cancellationToken));
            var allowedVmSizesUrl = new Uri(await storageAccessProvider.GetInternalTesBlobUrlAsync("/configuration/allowed-vm-sizes", cancellationToken));

            var supportedVmSizes = (await skuInformationProvider.GetVmSizesAndPricesAsync(batchAccountResourceInformation.Region, cancellationToken)).ToList();
            var batchAccountQuotas = await quotaProvider.GetVmCoreQuotaAsync(lowPriority: false, cancellationToken: cancellationToken);
            var supportedVmSizesFileContent = VirtualMachineInfoToFixedWidthColumns(supportedVmSizes.OrderBy(v => v.VmFamily).ThenBy(v => v.VmSize), batchAccountQuotas);

            try
            {
                await storageAccessProvider.UploadBlobAsync(supportedVmSizesUrl, supportedVmSizesFileContent, cancellationToken);
            }
            catch
            {
                logger.LogWarning($"Failed to write {supportedVmSizesUrl.AbsolutePath}. Updated VM size information will not be available in the configuration directory. This will not impact the workflow execution.");
            }

            var allowedVmSizesFileContent = await storageAccessProvider.DownloadBlobAsync(allowedVmSizesUrl, cancellationToken);

            if (allowedVmSizesFileContent is null)
            {
                logger.LogWarning($"Unable to read from {allowedVmSizesUrl.AbsolutePath}. All supported VM sizes will be eligible for Azure Batch task scheduling.");
                return new List<string>();
            }

            // Read the allowed-vm-sizes configuration file and remove any previous warnings (those start with "<" following the VM size or family name)
            var allowedVmSizesLines = allowedVmSizesFileContent
                .Split(new[] { '\r', '\n' })
                .Select(line => line.Split('<', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).FirstOrDefault() ?? string.Empty)
                .ToList();

            var allowedVmSizesWithoutComments = allowedVmSizesLines
                .Select(line => line.Trim())
                .Where(line => !string.IsNullOrWhiteSpace(line) && !line.StartsWith("#"))
                .ToList();

            var allowedAndSupportedVmSizes = allowedVmSizesWithoutComments.Intersect(supportedVmSizes.Select(v => v.VmSize), StringComparer.OrdinalIgnoreCase)
                .Union(allowedVmSizesWithoutComments.Intersect(supportedVmSizes.Select(v => v.VmFamily), StringComparer.OrdinalIgnoreCase))
                .Distinct()
                .ToList();

            var allowedVmSizesButNotSupported = allowedVmSizesWithoutComments.Except(allowedAndSupportedVmSizes).Distinct().ToList();

            if (allowedVmSizesButNotSupported.Any())
            {
                logger.LogWarning($"The following VM sizes or families are listed in {allowedVmSizesUrl.AbsolutePath}, but are either misspelled or not supported in your region: {string.Join(", ", allowedVmSizesButNotSupported)}. These will be ignored.");

                var linesWithWarningsAdded = allowedVmSizesLines.ConvertAll(line =>
                    allowedVmSizesButNotSupported.Contains(line, StringComparer.OrdinalIgnoreCase)
                        ? $"{line} <-- WARNING: This VM size or family is either misspelled or not supported in your region. It will be ignored."
                        : line
                );

                var allowedVmSizesFileContentWithWarningsAdded = string.Join('\n', linesWithWarningsAdded);

                if (allowedVmSizesFileContentWithWarningsAdded != allowedVmSizesFileContent)
                {
                    try
                    {
                        await storageAccessProvider.UploadBlobAsync(allowedVmSizesUrl, allowedVmSizesFileContentWithWarningsAdded, cancellationToken);
                    }
                    catch
                    {
                        logger.LogWarning($"Failed to write warnings to {allowedVmSizesUrl.AbsolutePath}.");
                    }
                }
            }

            return allowedAndSupportedVmSizes;
        }

        /// <summary>
        /// Combines the VM feature and price info with Batch quotas and produces the fixed-width list ready for uploading to .
        /// </summary>
        /// <param name="vmInfos">List of <see cref="VirtualMachineInformation"/></param>
        /// <param name="batchAccountQuotas">Batch quotas <see cref="AzureBatchAccountQuotas"/></param>
        /// <returns></returns>
        private static string VirtualMachineInfoToFixedWidthColumns(IEnumerable<VirtualMachineInformation> vmInfos, BatchVmCoreQuota batchAccountQuotas)
        {
            var vmSizes = vmInfos.Where(v => !v.LowPriority).Select(v => v.VmSize);

            var vmInfosAsStrings = vmSizes
                .Select(s => new { VmInfoWithDedicatedPrice = vmInfos.SingleOrDefault(l => l.VmSize == s && !l.LowPriority), PricePerHourLowPri = vmInfos.FirstOrDefault(l => l.VmSize == s && l.LowPriority)?.PricePerHour })
                .Select(v => new
                {
                    v.VmInfoWithDedicatedPrice.VmSize,
                    v.VmInfoWithDedicatedPrice.VmFamily,
                    PricePerHourDedicated = v.VmInfoWithDedicatedPrice.PricePerHour?.ToString("###0.000"),
                    PricePerHourLowPri = v.PricePerHourLowPri is not null ? v.PricePerHourLowPri?.ToString("###0.000") : "N/A",
                    MemoryInGiB = v.VmInfoWithDedicatedPrice.MemoryInGiB?.ToString(),
                    NumberOfCores = v.VmInfoWithDedicatedPrice.VCpusAvailable.ToString(),
                    ResourceDiskSizeInGiB = v.VmInfoWithDedicatedPrice.ResourceDiskSizeInGiB.ToString(),
                    DedicatedQuota = batchAccountQuotas.IsDedicatedAndPerVmFamilyCoreQuotaEnforced
                        ? batchAccountQuotas.DedicatedCoreQuotas.FirstOrDefault(q => q.VmFamilyName.Equals(v.VmInfoWithDedicatedPrice.VmFamily, StringComparison.OrdinalIgnoreCase))?.CoreQuota.ToString() ?? "N/A"
                        : batchAccountQuotas.NumberOfCores.ToString()
                });

            vmInfosAsStrings = vmInfosAsStrings.Prepend(new { VmSize = string.Empty, VmFamily = string.Empty, PricePerHourDedicated = "dedicated", PricePerHourLowPri = "low pri", MemoryInGiB = "(GiB)", NumberOfCores = string.Empty, ResourceDiskSizeInGiB = "(GiB)", DedicatedQuota = $"quota {(batchAccountQuotas.IsDedicatedAndPerVmFamilyCoreQuotaEnforced ? "(per fam.)" : "(total)")}" });
            vmInfosAsStrings = vmInfosAsStrings.Prepend(new { VmSize = "VM Size", VmFamily = "Family", PricePerHourDedicated = "$/hour", PricePerHourLowPri = "$/hour", MemoryInGiB = "Memory", NumberOfCores = "CPUs", ResourceDiskSizeInGiB = "Disk", DedicatedQuota = "Dedicated CPU" });

            var sizeColWidth = vmInfosAsStrings.Max(v => v.VmSize.Length);
            var seriesColWidth = vmInfosAsStrings.Max(v => v.VmFamily.Length);
            var priceDedicatedColumnWidth = vmInfosAsStrings.Max(v => v.PricePerHourDedicated.Length);
            var priceLowPriColumnWidth = vmInfosAsStrings.Max(v => v.PricePerHourLowPri.Length);
            var memoryColumnWidth = vmInfosAsStrings.Max(v => v.MemoryInGiB.Length);
            var coresColumnWidth = vmInfosAsStrings.Max(v => v.NumberOfCores.Length);
            var diskColumnWidth = vmInfosAsStrings.Max(v => v.ResourceDiskSizeInGiB.Length);
            var dedicatedQuotaColumnWidth = vmInfosAsStrings.Max(v => v.DedicatedQuota.Length);

            var fixedWidthVmInfos = vmInfosAsStrings.Select(v => $"{v.VmSize.PadRight(sizeColWidth)} {v.VmFamily.PadRight(seriesColWidth)} {v.PricePerHourDedicated.PadLeft(priceDedicatedColumnWidth)}  {v.PricePerHourLowPri.PadLeft(priceLowPriColumnWidth)}  {v.MemoryInGiB.PadLeft(memoryColumnWidth)}  {v.NumberOfCores.PadLeft(coresColumnWidth)}  {v.ResourceDiskSizeInGiB.PadLeft(diskColumnWidth)}  {v.DedicatedQuota.PadLeft(dedicatedQuotaColumnWidth)}");

            return string.Join('\n', fixedWidthVmInfos);
        }
    }
}
