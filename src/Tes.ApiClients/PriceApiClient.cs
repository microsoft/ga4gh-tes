// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Tes.ApiClients.Models.Pricing;

namespace Tes.ApiClients
{
    /// <summary>
    /// Azure Retail Price API client. 
    /// </summary>
    public class PriceApiClient : HttpApiClient
    {
        private static readonly Uri ApiEndpoint = new("https://prices.azure.com/api/retail/prices");

        /// <summary>
        /// Constructor of the Price API Client.
        /// </summary>
        /// <param name="cachingRetryHandler"><see cref="CachingRetryHandler"/></param>
        /// <param name="logger"><see cref="ILogger{TCategoryName}"/></param>
        public PriceApiClient(CachingRetryHandler cachingRetryHandler, ILogger<PriceApiClient> logger) : base(cachingRetryHandler, logger)
        {
        }

        /// <summary>
        /// Get pricing information in a region. 
        /// </summary>
        /// <param name="region">arm region</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="cacheResults">If true results will be cached. Default is false.</param>
        /// <returns>pricing items</returns>
        public async IAsyncEnumerable<PricingItem> GetAllPricingInformationAsync(string region, [EnumeratorCancellation] CancellationToken cancellationToken, bool cacheResults = false)
        {
            var skip = 0;

            while (true)
            {
                var page = await GetPricingInformationPageAsync(skip, region, cancellationToken, cacheResults);

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
        /// Returns pricing information for non Windows and non spot VM..
        /// </summary>
        /// <param name="region">arm region.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="cacheResults">If true results will be cached. Default is false.</param>
        /// <returns></returns>
        public IAsyncEnumerable<PricingItem> GetAllPricingInformationForNonWindowsAndNonSpotVmsAsync(string region, CancellationToken cancellationToken, bool cacheResults = false)
            => GetAllPricingInformationAsync(region, cancellationToken, cacheResults)
                .WhereAwait(p => ValueTask.FromResult(!p.productName.Contains(" Windows") && !p.meterName.Contains(" Spot")));

        /// <summary>
        /// Returns a page of pricing information starting at the requested position for a given region.
        /// </summary>
        /// <param name="skip">starting position.</param>
        /// <param name="region">arm region.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="cacheResults">If true results will be cached for the specific request. Default is false.</param>
        /// <returns></returns>
        public async Task<RetailPricingData> GetPricingInformationPageAsync(int skip, string region, CancellationToken cancellationToken, bool cacheResults = false)
        {
            var builder = new UriBuilder(ApiEndpoint) { Query = BuildRequestQueryString(skip, region) };

            var result = await HttpGetRequestAsync<RetailPricingData>(builder.Uri, setAuthorizationHeader: false, cacheResults: cacheResults, cancellationToken: cancellationToken);

            result.RequestLink = builder.ToString();

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
            => $"$filter={String.Join($" {conditionOperator} ", condition)}";

        private static string ParseQueryStringKeyIntValue(string key, int value)
            => $"{key}={value}";

        private static string ParseEq(string name, string value)
            => $"{name} eq '{value}'";

        private static string ParseEq(string name, bool value)
            => $"{name} eq {value.ToString().ToLowerInvariant()}";
    }
}
