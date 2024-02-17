// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading;
using System.Threading.Tasks;
using CommonUtilities;
using Azure.Identity;
using CommonUtilities.AzureCloud;
using Microsoft.Azure.Management.Batch;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
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
        private readonly AzureCloudConfig azureCloudConfig;

        /// <summary>
        /// Batch account resource information.
        /// </summary>
        public BatchAccountResourceInformation BatchAccountInformation => batchAccountInformation;

        /// <summary>
        /// Constructor of AzureManagementClientsFactory
        /// </summary>
        /// <param name="batchAccountInformation"><see cref="BatchAccountResourceInformation"/>></param>
        /// <param name="azureCloudConfig"></param>
        /// <exception cref="ArgumentException"></exception>
        public AzureManagementClientsFactory(BatchAccountResourceInformation batchAccountInformation, AzureCloudConfig azureCloudConfig)
        {
            ArgumentNullException.ThrowIfNull(batchAccountInformation);
            ArgumentNullException.ThrowIfNull(azureCloudConfig);

            if (string.IsNullOrEmpty(batchAccountInformation.SubscriptionId))
            {
                throw new ArgumentException("Batch account information does not contain the subscription id. ", nameof(batchAccountInformation));
            }

            if (string.IsNullOrEmpty(batchAccountInformation.ResourceGroupName))
            {
                throw new ArgumentException("Batch account information does not contain the resource group name.", nameof(batchAccountInformation));
            }

            this.batchAccountInformation = batchAccountInformation;
            this.azureCloudConfig = azureCloudConfig;
        }

        /// <summary>
        /// Protected constructor.
        /// </summary>
        protected AzureManagementClientsFactory() { }

        private static async Task<string> GetAzureAccessTokenAsync(CancellationToken cancellationToken, string scope = "https://management.azure.com//.default")
            => (await (new DefaultAzureCredential()).GetTokenAsync(new Azure.Core.TokenRequestContext(new string[] { scope }), cancellationToken)).Token;

        /// <summary>
        /// Creates Batch Account management client using AAD authentication.
        /// Configure to the subscription id that contains the batch account.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public async Task<BatchManagementClient> CreateBatchAccountManagementClient(CancellationToken cancellationToken)
            => new BatchManagementClient(new TokenCredentials(await GetAzureAccessTokenAsync(cancellationToken, resource: azureCloudConfig.ResourceManagerUrl))) { SubscriptionId = batchAccountInformation.SubscriptionId };

        /// <summary>
        /// Creates a new instance of Azure Management Client with the default credentials and subscription.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public async Task<FluentAzure.IAuthenticated> CreateAzureManagementClientAsync(CancellationToken cancellationToken)
            => await GetAzureManagementClientAsync(azureCloudConfig, cancellationToken);

        /// <summary>
        /// Creates a new instance of Azure Management client
        /// </summary>
        /// <param name="azureCloudIdentityConfig"></param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public static async Task<FluentAzure.IAuthenticated> GetAzureManagementClientAsync(AzureCloudConfig azureCloudConfig, CancellationToken cancellationToken)
        {
            var accessToken = await GetAzureAccessTokenAsync(cancellationToken, resource: azureCloudConfig.ResourceManagerUrl);
            var azureCredentials = new AzureCredentials(new TokenCredentials(accessToken), null, null, azureCloudConfig.AzureEnvironment);
            var azureClient = FluentAzure.Authenticate(azureCredentials);

            return azureClient;
        }
    }
}
