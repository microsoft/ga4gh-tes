// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Management.Batch;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Rest;
using FluentAzure = Microsoft.Azure.Management.Fluent.Azure;


namespace TesApi.Web.Management
{
    /// <summary>
    /// Factory if ARM management clients. 
    /// </summary>
    public class AzureManagementClientsFactory
    {
        private readonly BatchAccountResourceInformation batchAccountInformation;

        /// <summary>
        /// Batch account resource information.
        /// </summary>
        public BatchAccountResourceInformation BatchAccountInformation => batchAccountInformation;

        /// <summary>
        /// Constructor of AzureManagementClientsFactory
        /// </summary>
        /// <param name="batchAccountInformation"><see cref="BatchAccountResourceInformation"/>></param>
        /// <exception cref="ArgumentException"></exception>
        public AzureManagementClientsFactory(BatchAccountResourceInformation batchAccountInformation)
        {
            ArgumentNullException.ThrowIfNull(batchAccountInformation);

            if (string.IsNullOrEmpty(batchAccountInformation.SubscriptionId))
            {
                throw new ArgumentException("Batch account information does not contain the subscription id. ", nameof(batchAccountInformation));
            }

            if (string.IsNullOrEmpty(batchAccountInformation.ResourceGroupName))
            {
                throw new ArgumentException("Batch account information does not contain the resource group name.", nameof(batchAccountInformation));
            }

            this.batchAccountInformation = batchAccountInformation;
        }

        /// <summary>
        /// Protected constructor.
        /// </summary>
        protected AzureManagementClientsFactory() { }

        private static Task<string> GetAzureAccessTokenAsync(string resource = "https://management.azure.com/")
            => new AzureServiceTokenProvider().GetAccessTokenAsync(resource);

        /// <summary>
        /// Creates Batch Account management client using AAD authentication.
        /// Configure to the subscription id that contains the batch account.
        /// </summary>
        /// <returns></returns>
        public async Task<BatchManagementClient> CreateBatchAccountManagementClient()
            => new BatchManagementClient(new TokenCredentials(await GetAzureAccessTokenAsync())) { SubscriptionId = batchAccountInformation.SubscriptionId };

        /// <summary>
        /// Creates a new instance of Azure Management Client with the default credentials and subscription.
        /// </summary>
        /// <returns></returns>
        public async Task<FluentAzure.IAuthenticated> CreateAzureManagementClientAsync()
            => await AzureManagementClientsFactory.GetAzureManagementClientAsync();

        /// <summary>
        /// Attempts to get the batch resource information using the ARM api.
        /// Returns null if the resource was not found or the account does not have access.
        /// </summary>
        /// <param name="batchAccountName">batch account name</param>
        /// <returns></returns>
        public static async Task<BatchAccountResourceInformation> TryGetResourceInformationFromAccountNameAsync(string batchAccountName)
        {
            //TODO: look if a newer version of the management SDK provides a simpler way to look for this information .
            var tokenCredentials = new TokenCredentials(await GetAzureAccessTokenAsync());
            var azureClient = await GetAzureManagementClientAsync();

            var subscriptionIds = (await azureClient.Subscriptions.ListAsync()).Select(s => s.SubscriptionId);

            foreach (var subId in subscriptionIds)
            {
                using var batchClient = new BatchManagementClient(tokenCredentials) { SubscriptionId = subId };

                var batchAccount = (await batchClient.BatchAccount.ListAsync())
                    .FirstOrDefault(a => a.Name.Equals(batchAccountName, StringComparison.OrdinalIgnoreCase));

                if (batchAccount is not null)
                {
                    return BatchAccountResourceInformation.FromBatchResourceId(batchAccount.Id, batchAccount.Location);
                }
            }

            return null;
        }

        private static async Task<FluentAzure.IAuthenticated> GetAzureManagementClientAsync()
        {
            var accessToken = await GetAzureAccessTokenAsync();
            var azureCredentials = new AzureCredentials(new TokenCredentials(accessToken), null, null, AzureEnvironment.AzureGlobalCloud);
            var azureClient = FluentAzure.Authenticate(azureCredentials);

            return azureClient;
        }
    }
}
