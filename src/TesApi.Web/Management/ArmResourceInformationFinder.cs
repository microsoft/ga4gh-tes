// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Management.ApplicationInsights.Management;
using Microsoft.Azure.Management.Batch;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Rest;

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
        /// <param name="accountName"></param>
        /// <returns></returns>
        /// <param name="cancellationToken"></param>
        public static Task<string> GetAppInsightsConnectionStringAsync(string accountName, CancellationToken cancellationToken)
        {
            return GetAzureResourceAsync(
                clientFactory: (tokenCredentials, subscription) => new ApplicationInsightsManagementClient(tokenCredentials) { SubscriptionId = subscription },
                listAsync: (client, ct) => client.Components.ListAsync(ct),
                listNextAsync: (client, link, ct) => client.Components.ListNextAsync(link, ct),
                predicate: a => a.ApplicationId.Equals(accountName, StringComparison.OrdinalIgnoreCase),
                cancellationToken: cancellationToken,
                finalize: a => a.InstrumentationKey);
        }

        /// <summary>
        /// Attempts to get the batch resource information using the ARM api.
        /// Returns null if the resource was not found or the account does not have access.
        /// </summary>
        /// <param name="batchAccountName">batch account name</param>
        /// <returns></returns>
        /// <param name="cancellationToken"></param>
        public static Task<BatchAccountResourceInformation> TryGetResourceInformationFromAccountNameAsync(string batchAccountName, CancellationToken cancellationToken)
        {
            //TODO: look if a newer version of the management SDK provides a simpler way to look for this information .
            return GetAzureResourceAsync(
                clientFactory: (tokenCredentials, subscription) => new BatchManagementClient(tokenCredentials) { SubscriptionId = subscription },
                listAsync: (client, ct) => client.BatchAccount.ListAsync(ct),
                listNextAsync: (client, link, ct) => client.BatchAccount.ListNextAsync(link, ct),
                predicate: a => a.Name.Equals(batchAccountName, StringComparison.OrdinalIgnoreCase),
                cancellationToken: cancellationToken,
                finalize: batchAccount => BatchAccountResourceInformation.FromBatchResourceId(batchAccount.Id, batchAccount.Location, $"https://{batchAccount.AccountEndpoint}"));
        }

        //TODO: refactor this to use Azure Identity token provider. 
        private static Task<string> GetAzureAccessTokenAsync(string resource = "https://management.azure.com/")
        {
            return new AzureServiceTokenProvider().GetAccessTokenAsync(resource);
        }

        /// <summary>
        /// Looks up an Azure resource with management clients that use <see cref="Microsoft.Rest.Azure.IPage{T}"/> enumerators
        /// </summary>
        /// <typeparam name="TResult">Value to return</typeparam>
        /// <typeparam name="TAzManagementClient">Type of Azure management client to use to locate resources of type <typeparamref name="TResource"/></typeparam>
        /// <typeparam name="TResource">Type of Azure resource to enumerate/locate</typeparam>
        /// <param name="clientFactory">Returns management client appropriate for enumerating resources of <typeparamref name="TResource"/>. A <see cref="TokenCredentials"/> and the <c>SubscriptionId</c> are passed to this method as parameters.</param>
        /// <param name="listAsync"><c>ListAsync</c> method from operational parameter on <typeparamref name="TAzManagementClient"/>. Parameters are the <typeparamref name="TAzManagementClient"/> returned by <paramref name="clientFactory"/> and <paramref name="cancellationToken"/>.</param>
        /// <param name="listNextAsync"><c>ListNextAsync</c> method from operational parameter on <typeparamref name="TAzManagementClient"/>. Parameters are the <typeparamref name="TAzManagementClient"/> returned by <paramref name="clientFactory"/>, the <see cref="Microsoft.Rest.Azure.IPage{T}.NextPageLink"/> from the previous server call, and <paramref name="cancellationToken"/>.</param>
        /// <param name="predicate">Returns true when the desired <typeparamref name="TResource"/> is found.</param>
        /// <param name="cancellationToken"></param>
        /// <param name="finalize">Converts <typeparamref name="TResource"/> to <typeparamref name="TResult"/>. Required if <typeparamref name="TResource"/> is not <typeparamref name="TResult"/>.</param>
        /// <returns>The <typeparamref name="TResult"/> derived from the first <typeparamref name="TResource"/> that satisfies the condition in <paramref name="predicate"/>, else <c>default</c>.</returns>
        private static async Task<TResult> GetAzureResourceAsync<TResult, TAzManagementClient, TResource>(
                Func<TokenCredentials, string, TAzManagementClient> clientFactory,
                Func<TAzManagementClient, CancellationToken, Task<Microsoft.Rest.Azure.IPage<TResource>>> listAsync,
                Func<TAzManagementClient, string, CancellationToken, Task<Microsoft.Rest.Azure.IPage<TResource>>> listNextAsync,
                Predicate<TResource> predicate,
                CancellationToken cancellationToken,
                Func<TResource, TResult> finalize = default)
            where TAzManagementClient : Microsoft.Rest.Azure.IAzureClient, IDisposable
        {
            if (typeof(TResult) == typeof(TResource))
            {
                finalize ??= new(a => (TResult)Convert.ChangeType(a, typeof(TResult)));
            }

            ArgumentNullException.ThrowIfNull(clientFactory);
            ArgumentNullException.ThrowIfNull(listAsync);
            ArgumentNullException.ThrowIfNull(listNextAsync);
            ArgumentNullException.ThrowIfNull(predicate);
            ArgumentNullException.ThrowIfNull(finalize);

            var tokenCredentials = new TokenCredentials(await GetAzureAccessTokenAsync());
            var azureManagementClient = await AzureManagementClientsFactory.GetAzureManagementClientAsync(cancellationToken);

            var subscriptions = (await azureManagementClient.Subscriptions.ListAsync(cancellationToken: cancellationToken)).ToAsyncEnumerable().Select(s => s.SubscriptionId);

            await foreach (var subId in subscriptions.WithCancellation(cancellationToken))
            {
                using var client = clientFactory(tokenCredentials, subId);

                var item = await (await listAsync(client, cancellationToken))
                    .ToAsyncEnumerable((page, ct) => listNextAsync(client, page, ct))
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
