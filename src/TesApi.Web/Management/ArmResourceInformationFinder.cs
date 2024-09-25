// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.ResourceManager;

namespace TesApi.Web.Management
{
    /// <summary>
    /// Provides utility methods to find resource information using the TES service identity
    /// </summary>
    public static class ArmResourceInformationFinder
    {
        /// <summary>
        /// Looks up the AppInsights instrumentation key in subscriptions the TES services has access to 
        /// </summary>
        /// <param name="accountName">AppInsights account name</param>
        /// <param name="tokenCredential">A credential capable of providing an OAuth token.</param>
        /// <param name="armEnvironment">The information of an Azure Cloud environment.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public static Task<string> GetAppInsightsConnectionStringFromAccountNameAsync(string accountName, Azure.Core.TokenCredential tokenCredential, ArmEnvironment armEnvironment, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrEmpty(accountName);

            return GetAzureResourceAsync(
                tokenCredential, armEnvironment,
                listAsync: Azure.ResourceManager.ApplicationInsights.ApplicationInsightsExtensions.GetApplicationInsightsComponentsAsync,
                getDataAsync: async (subscriptionResource, token) => await subscriptionResource.GetAsync(token),
                getData: subscriptionResource => subscriptionResource.Data,
                predicate: a => a.ApplicationId.Equals(accountName, StringComparison.OrdinalIgnoreCase),
                cancellationToken: cancellationToken,
                finalize: a => a.ConnectionString);
        }

        /// <summary>
        /// Attempts to get the batch resource information using the ARM api.
        /// Returns null if the resource was not found or the account does not have access.
        /// </summary>
        /// <param name="batchAccountName">Batch account name</param>
        /// <param name="tokenCredential">A credential capable of providing an OAuth token.</param>
        /// <param name="armEnvironment">The information of an Azure Cloud environment.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public static Task<BatchAccountResourceInformation> TryGetBatchAccountInformationFromAccountNameAsync(string batchAccountName, Azure.Core.TokenCredential tokenCredential, ArmEnvironment armEnvironment, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrEmpty(batchAccountName);

            return GetAzureResourceAsync(
                tokenCredential, armEnvironment,
                listAsync: Azure.ResourceManager.Batch.BatchExtensions.GetBatchAccountsAsync,
                getDataAsync: async (subscriptionResource, token) => await subscriptionResource.GetAsync(token),
                getData: subscriptionResource => subscriptionResource.Data,
                predicate: a => a.Name.Equals(batchAccountName, StringComparison.OrdinalIgnoreCase),
                cancellationToken: cancellationToken,
                finalize: batchAccount => BatchAccountResourceInformation.FromBatchResourceId(batchAccount.Id.ToString(), batchAccount.Location?.Name, $"{Uri.UriSchemeHttps}://{batchAccount.AccountEndpoint}"));
        }

        /// <summary>
        /// Looks up an Azure resource with management clients that use <see cref="Microsoft.Rest.Azure.IPage{T}"/> enumerators
        /// </summary>
        /// <typeparam name="TResult">Value to return</typeparam>
        /// <typeparam name="TResource">Type of Azure resource to enumerate/locate</typeparam>
        /// <typeparam name="TResourceData">Type of Azure resource data</typeparam>
        /// <param name="tokenCredential">A credential capable of providing an OAuth token.</param>
        /// <param name="armEnvironment">The information of an Azure Cloud environment.</param>
        /// <param name="listAsync"><c>Get{TResource}sAsync</c>-style extension method with <c>this</c> parameter of type <see cref="Azure.ResourceManager.Resources.SubscriptionResource"/> and one other parameter <paramref name="cancellationToken"/>.</param>
        /// <param name="getDataAsync"><c>GetAsync</c>-style extension method with <c>this</c> parameter of <typeparamref name="TResource"/> and one parameter <paramref name="cancellationToken"/>.</param>
        /// <param name="getData">Accessor to the <c>Data</c> property of <typeparamref name="TResource"/>, expected to return a <typeparamref name="TResourceData"/>. Exists to avoid using reflection.</param>
        /// <param name="predicate">Returns true when the desired <typeparamref name="TResourceData"/> is found.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <param name="finalize">Converts <typeparamref name="TResourceData"/> to <typeparamref name="TResult"/>. Required if <typeparamref name="TResourceData"/> is not <typeparamref name="TResult"/>.</param>
        /// <returns>The <typeparamref name="TResult"/> derived from the first <typeparamref name="TResource"/> that satisfies the condition in <paramref name="predicate"/>, else <c>default</c>.</returns>
        private static async Task<TResult> GetAzureResourceAsync<TResult, TResource, TResourceData>(
                Azure.Core.TokenCredential tokenCredential,
                ArmEnvironment armEnvironment,
                Func<Azure.ResourceManager.Resources.SubscriptionResource, CancellationToken, AsyncPageable<TResource>> listAsync,
                Func<TResource, CancellationToken, Task<Response<TResource>>> getDataAsync,
                Func<TResource, TResourceData> getData,
                Predicate<TResourceData> predicate,
                CancellationToken cancellationToken,
                Func<TResourceData, TResult> finalize = default)
            where TResource : ArmResource
        {
            if (typeof(TResult) == typeof(TResource))
            {
                finalize ??= new(a => (TResult)Convert.ChangeType(a, typeof(TResult)));
            }

            ArgumentNullException.ThrowIfNull(tokenCredential);
            ArgumentNullException.ThrowIfNull(armEnvironment);
            ArgumentNullException.ThrowIfNull(listAsync);
            ArgumentNullException.ThrowIfNull(getDataAsync);
            ArgumentNullException.ThrowIfNull(getData);
            ArgumentNullException.ThrowIfNull(predicate);
            ArgumentNullException.ThrowIfNull(finalize);

            var armClient = new ArmClient(tokenCredential, null, new ArmClientOptions { Environment = armEnvironment });

            await foreach (var subResource in armClient.GetSubscriptions().SelectAwaitWithCancellation(async (sub, token) => (await sub.GetAsync(token)).Value).WithCancellation(CancellationToken.None))
            {
                var item = await listAsync(subResource, cancellationToken)
                    .SelectAwaitWithCancellation(async (subscriptionResource, token) => await getDataAsync(subscriptionResource, token))
                    .Select(response => getData(response.Value))
                    .FirstOrDefaultAsync(a => predicate(a), cancellationToken);

                if (item is not null)
                {
                    return finalize(item);
                }
            }

            return default;
        }
    }
}
