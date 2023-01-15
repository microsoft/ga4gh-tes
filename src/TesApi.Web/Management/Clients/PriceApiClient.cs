// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using TesApi.Web.Management.Models.Pricing;

namespace TesApi.Web.Management.Clients
{
    /// <summary>
    /// Azure Retail Price API client. 
    /// </summary>
    public class PriceApiClient : HttpApiClient
    {
        private static readonly Uri ApiEndpoint = new Uri("https://prices.azure.com/api/retail/prices");

        /// <summary>
        /// Constructor of the Price API Client.
        /// </summary>
        /// <param name="cacheAndRetryHandler"><see cref="CacheAndRetryHandler"/></param>
        /// <param name="logger"><see cref="ILogger{TCategoryName}"/></param>
        public PriceApiClient(CacheAndRetryHandler cacheAndRetryHandler, ILogger<HttpApiClient> logger) : base(cacheAndRetryHandler, logger)
        {
        }

        /// <summary>
        /// Get pricing information in a region. 
        /// </summary>
        /// <param name="region">arm region</param>
        /// <param name="cacheResults">If true results will be cached. Default is false.</param>
        /// <returns>pricing items</returns>
        public async IAsyncEnumerable<PricingItem> GetAllPricingInformationAsync(string region, bool cacheResults = false)
        {
            var skip = 0;
            while (true)
            {
                var page = await GetPricingInformationPageAsync(skip, region, cacheResults);

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
        /// Returns pricing information for non Windows and non spot VM . 
        /// </summary>
        /// <param name="region">arm region.</param>
        /// <param name="cacheResults">If true results will be cached. Default is false.</param>
        /// <returns></returns>
        public IAsyncEnumerable<PricingItem> GetAllPricingInformationForNonWindowsAndNonSpotVmsAsync(string region, bool cacheResults = false)
        {
            return GetAllPricingInformationAsync(region, cacheResults)
                .WhereAwait(p => ValueTask.FromResult(!p.productName.Contains(" Windows") && !p.meterName.Contains(" Spot")));
        }

        /// <summary>
        /// Returns a page of pricing information starting at the requested position for a given region.
        /// </summary>
        /// <param name="skip">starting position.</param>
        /// <param name="region">arm region.</param>
        /// <param name="cacheResults">If true results will be cached for the specific request. Default is false.</param>
        /// <returns></returns>
        public async Task<RetailPricingData> GetPricingInformationPageAsync(int skip, string region, bool cacheResults = false)
        {
            var builder = new UriBuilder(ApiEndpoint) { Query = BuildRequestQueryString(skip, region) };

            var result = await HttpGetRequestAsync<RetailPricingData>(builder.Uri, setAuthorizationHeader: false, cacheResults);

            if (result is not null)
            {
                result.RequestLink = builder.ToString();
            }

            return result;
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
        {
            return $"$filter={String.Join($" {conditionOperator} ", condition)}";
        }

        private static string ParseQueryStringKeyIntValue(string key, int value)
        {
            return $"{key}={value}";
        }

        private static string ParseEq(string name, string value)
        {
            return $"{name} eq '{value}'";
        }
        private static string ParseEq(string name, bool value)
        {
            return $"{name} eq {value.ToString().ToLowerInvariant()}";
        }
    }
}
