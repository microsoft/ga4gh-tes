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
        public static async Task<string> GetAppInsightsConnectionStringAsync(string accountName, CancellationToken cancellationToken)
        {
            var azureClient = await AzureManagementClientsFactory.GetAzureManagementClientAsync(cancellationToken);
            var subscriptionIds = (await azureClient.Subscriptions.ListAsync(cancellationToken: cancellationToken)).ToAsyncEnumerable().Select(s => s.SubscriptionId);

            var credentials = new TokenCredentials(await GetAzureAccessTokenAsync());

            await foreach (var subscriptionId in subscriptionIds.WithCancellation(cancellationToken))
            {
                using var appInsightsClient = new ApplicationInsightsManagementClient(credentials) { SubscriptionId = subscriptionId };
                var app = await (await appInsightsClient.Components.ListAsync(cancellationToken))
                    .ToAsyncEnumerable(appInsightsClient.Components.ListNextAsync)
                    .FirstOrDefaultAsync(a => a.ApplicationId.Equals(accountName, StringComparison.OrdinalIgnoreCase), cancellationToken);

                if (app is not null)
                {
                    return app.InstrumentationKey;
                }
            }

            return null;
        }

        //TODO: refactor this to use Azure Identity token provider. 
        private static Task<string> GetAzureAccessTokenAsync(string resource = "https://management.azure.com/")
        {
            return new AzureServiceTokenProvider().GetAccessTokenAsync(resource);
        }

        /// <summary>
        /// Attempts to get the batch resource information using the ARM api.
        /// Returns null if the resource was not found or the account does not have access.
        /// </summary>
        /// <param name="batchAccountName">batch account name</param>
        /// <returns></returns>
        /// <param name="cancellationToken"></param>
        public static async Task<BatchAccountResourceInformation> TryGetResourceInformationFromAccountNameAsync(string batchAccountName, CancellationToken cancellationToken)
        {
            //TODO: look if a newer version of the management SDK provides a simpler way to look for this information .
            var tokenCredentials = new TokenCredentials(await GetAzureAccessTokenAsync());
            var azureClient = await AzureManagementClientsFactory.GetAzureManagementClientAsync(cancellationToken);

            var subscriptionIds = (await azureClient.Subscriptions.ListAsync(cancellationToken: cancellationToken))
                .ToAsyncEnumerable().Select(s => s.SubscriptionId);

            await foreach (var subId in subscriptionIds.WithCancellation(cancellationToken))
            {
                using var batchClient = new BatchManagementClient(tokenCredentials) { SubscriptionId = subId };

                var batchAccount = await (await batchClient.BatchAccount.ListAsync(cancellationToken))
                    .ToAsyncEnumerable(batchClient.BatchAccount.ListNextAsync)
                    .FirstOrDefaultAsync(a => a.Name.Equals(batchAccountName, StringComparison.OrdinalIgnoreCase), cancellationToken);

                if (batchAccount is not null)
                {
                    return BatchAccountResourceInformation.FromBatchResourceId(batchAccount.Id, batchAccount.Location, $"https://{batchAccount.AccountEndpoint}");
                }
            }

            return null;
        }
    }
}
