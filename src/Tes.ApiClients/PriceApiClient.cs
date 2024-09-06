// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Tes.ApiClients.Models.Pricing;

namespace Tes.ApiClients
{
    /// <summary>
    /// Azure Retail Price API client. 
    /// </summary>
    /// <remarks>
    /// Constructor of the Price API Client.
    /// </remarks>
    /// <param name="cachingRetryPolicyBuilder"><see cref="CachingRetryPolicyBuilder"/></param>
    /// <param name="logger"><see cref="ILogger{TCategoryName}"/></param>
    public class PriceApiClient(CachingRetryPolicyBuilder cachingRetryPolicyBuilder, ILogger<PriceApiClient> logger) : HttpApiClient(cachingRetryPolicyBuilder, logger)
    {
        private static readonly Uri ApiEndpoint = new("https://prices.azure.com/api/retail/prices");

        /// <summary>
        /// Get pricing information in a region. 
        /// </summary>
        /// <param name="serviceName">azure service.</param>
        /// <param name="region">arm region</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>pricing items</returns>
        public async IAsyncEnumerable<PricingItem> GetAllPricingInformationAsync(string serviceName, string region, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var skip = 0;

            while (true)
            {
                var page = await GetPricingInformationPageAsync(skip, serviceName, region, cancellationToken);

                if (page is null || page.Items is null || page.Items.Length == 0)
                {
                    yield break;
                }

                foreach (var pricingItem in page.Items)
                {
                    yield return pricingItem;
                }

                skip += 100;
            }
        }

        /// <summary>
        /// Returns pricing information for non Windows and non spot VM.
        /// </summary>
        /// <param name="region">arm region.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public IAsyncEnumerable<PricingItem> GetAllPricingInformationForNonWindowsAndNonSpotVmsAsync(string region, CancellationToken cancellationToken)
            => GetAllPricingInformationAsync("Virtual Machines", region, cancellationToken)
                .Where(p => !p.productName.Contains(" Windows") && !p.meterName.Contains(" Spot"));

        /// <summary>
        /// Returns pricing information for non Windows and non spot VM.
        /// </summary>
        /// <param name="region">arm region.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public IAsyncEnumerable<PricingItem> GetAllPricingInformationForStandardStorageLRSDisksAsync(string region, CancellationToken cancellationToken)
            => GetAllPricingInformationAsync("Storage", region, cancellationToken)
                .Where(p => p.productName.Equals("Standard SSD Managed Disks") && p.meterName.StartsWith('E') && p.meterName.EndsWith(" LRS Disk"));

        /// <summary>
        /// Returns a page of pricing information starting at the requested position for a given region.
        /// </summary>
        /// <param name="skip">starting position.</param>
        /// <param name="serviceName">azure service.</param>
        /// <param name="region">arm region.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public async Task<RetailPricingData> GetPricingInformationPageAsync(int skip, string serviceName, string region, CancellationToken cancellationToken)
        {
            var builder = new UriBuilder(ApiEndpoint) { Query = BuildRequestQueryString(skip, serviceName, region) };

            var result = await HttpGetRequestAsync(builder.Uri, setAuthorizationHeader: false, cacheResults: false, typeInfo: RetailPricingDataContext.Default.RetailPricingData, cancellationToken: cancellationToken);

            result.RequestLink = builder.ToString();

            return result;
        }

        private static string BuildRequestQueryString(int skip, string serviceName, string region)
        {
            var filter = ParseFilterCondition("and",
                ParseEq("serviceName", serviceName),
                ParseEq("currencyCode", "USD"),
                ParseEq("priceType", "Consumption"),
                ParseEq("armRegionName", region),
                ParseEq("isPrimaryMeterRegion", true));

            var skipKeyValue = ParseQueryStringKeyIntValue("$skip", skip);

            return $"{filter}&{skipKeyValue}";
        }

        private static string ParseFilterCondition(string conditionOperator, params string[] condition)
            => $"$filter={string.Join($" {conditionOperator} ", condition)}";

        private static string ParseQueryStringKeyIntValue(string key, int value)
            => $"{key}={value}";

        private static string ParseEq(string name, string value)
            => $"{name} eq '{value}'";

        private static string ParseEq(string name, bool value)
            => $"{name} eq {value.ToString().ToLowerInvariant()}";
    }
}
