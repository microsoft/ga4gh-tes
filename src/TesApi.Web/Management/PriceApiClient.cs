// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using TesApi.Web.Management.Models.Pricing;

namespace TesApi.Web.Management
{
    /// <summary>
    /// Azure Retail Price API client.
    /// </summary>
    public class PriceApiClient
    {
        private static readonly HttpClient httpClient = new();
        private static readonly Uri apiEndpoint = new($"https://prices.azure.com/api/retail/prices");

        /// <summary>
        /// Get pricing information in a region.
        /// </summary>
        /// <param name="region">arm region</param>
        /// <returns>pricing items</returns>
        public async IAsyncEnumerable<PricingItem> GetAllPricingInformationAsync(string region)
        {
            var skip = 0;
            while (true)
            {
                var page = await GetPricingInformationPageAsync(skip, region);

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
        /// <returns></returns>
        public IAsyncEnumerable<PricingItem> GetAllPricingInformationForNonWindowsAndNonSpotVmsAsync(string region)
            => GetAllPricingInformationAsync(region)
                .WhereAwait(p => ValueTask.FromResult(!p.productName.Contains(" Windows") && !p.meterName.Contains(" Spot")));

        /// <summary>
        /// Returns a page of pricing information starting at the requested position for a given region.
        /// </summary>
        /// <param name="skip">starting position.</param>
        /// <param name="region">arm region.</param>
        /// <returns></returns>
        public async Task<RetailPricingData> GetPricingInformationPageAsync(int skip, string region)
        {
            var builder = new UriBuilder(apiEndpoint)
            {
                Query = BuildRequestQueryString(skip, region)
            };

            var response = await httpClient.GetStringAsync(builder.Uri);

            if (string.IsNullOrWhiteSpace(response))
            {
                return null;
            }

            return JsonSerializer.Deserialize<RetailPricingData>(response);
        }

        private static string BuildRequestQueryString(int skip, string region)
        {
            var filter = ParseFilterCondition("and",
                ParseEq("serviceName", "Virtual Machines"),
                ParseEq("currencyCode", "USD"),
                ParseEq("priceType", "Consumption"),
                ParseEq("armRegionName", region),
                ParseEq("isPrimaryMeterRegion", true));

            var skipKeyValue = ParseQueryStringKeyIntValue("$skip", skip);

            return $"{filter}&{skipKeyValue}";

        }

        private static string ParseFilterCondition(string conditionOperator, params string[] condition)
            => $"$filter={String.Join($" {conditionOperator} ", condition)}";

        private static string ParseQueryStringKeyIntValue(string key, int value)
            => $"{key}={value}";

        private static string ParseEq(string name, string value)
            => $"{name} eq '{value}'";

        private static string ParseEq(string name, bool value)
            => $"{name} eq {value.ToString().ToLowerInvariant()}";
    }
}
