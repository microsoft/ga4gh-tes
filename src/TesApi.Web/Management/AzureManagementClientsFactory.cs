// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
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

        private static Task<string> GetAzureAccessTokenAsync(System.Threading.CancellationToken cancellationToken, string resource = "https://management.azure.com/")
            => new AzureServiceTokenProvider().GetAccessTokenAsync(resource, cancellationToken: cancellationToken);

        /// <summary>
        /// Creates Batch Account management client using AAD authentication.
        /// Configure to the subscription id that contains the batch account.
        /// </summary>
        /// <returns></returns>
        public async Task<BatchManagementClient> CreateBatchAccountManagementClient(System.Threading.CancellationToken cancellationToken)
            => new BatchManagementClient(new TokenCredentials(await GetAzureAccessTokenAsync(cancellationToken))) { SubscriptionId = batchAccountInformation.SubscriptionId };

        /// <summary>
        /// Creates a new instance of Azure Management Client with the default credentials and subscription.
        /// </summary>
        /// <returns></returns>
        public async Task<FluentAzure.IAuthenticated> CreateAzureManagementClientAsync(System.Threading.CancellationToken cancellationToken)
            => await AzureManagementClientsFactory.GetAzureManagementClientAsync(cancellationToken);

        /// <summary>
        /// Creates a new instance of Azure Management client
        /// </summary>
        /// <returns></returns>
        public static async Task<FluentAzure.IAuthenticated> GetAzureManagementClientAsync(System.Threading.CancellationToken cancellationToken)
        {
            var accessToken = await GetAzureAccessTokenAsync(cancellationToken);
            var azureCredentials = new AzureCredentials(new TokenCredentials(accessToken), null, null, AzureEnvironment.AzureGlobalCloud);
            var azureClient = FluentAzure.Authenticate(azureCredentials);

            return azureClient;
        }
    }
}
