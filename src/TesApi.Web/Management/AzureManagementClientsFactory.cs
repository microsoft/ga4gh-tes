// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading;
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

        private static Task<string> GetAzureAccessTokenAsync(CancellationToken cancellationToken, string resource = "https://management.azure.com/")
            => new AzureServiceTokenProvider().GetAccessTokenAsync(resource, cancellationToken: cancellationToken);

        /// <summary>
        /// Creates Batch Account management client using AAD authentication.
        /// Configure to the subscription id that contains the batch account.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        public async Task<BatchManagementClient> CreateBatchAccountManagementClient(CancellationToken cancellationToken)
            => new BatchManagementClient(new TokenCredentials(await GetAzureAccessTokenAsync(cancellationToken))) { SubscriptionId = batchAccountInformation.SubscriptionId };
    }
}
