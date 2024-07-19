// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using Azure.ResourceManager;
using Azure.ResourceManager.Batch;
using CommonUtilities;
using CommonUtilities.AzureCloud;


namespace TesApi.Web.Management
{
    /// <summary>
    /// Factory of ARM management clients. 
    /// </summary>
    public class AzureManagementClientsFactory
    {
        private readonly BatchAccountResourceInformation batchAccountInformation;
        private readonly AzureCloudConfig azureCloudConfig;
        private readonly AzureServicesConnectionStringCredentialOptions credentialOptions;

        /// <summary>
        /// Batch account resource information.
        /// </summary>
        public BatchAccountResourceInformation BatchAccountInformation => batchAccountInformation;

        /// <summary>
        /// Constructor of AzureManagementClientsFactory
        /// </summary>
        /// <param name="batchAccountInformation"><see cref="BatchAccountResourceInformation"/>></param>
        /// <param name="azureCloudConfig"></param>
        /// <param name="credentialOptions"></param>
        /// <exception cref="ArgumentException"></exception>
        public AzureManagementClientsFactory(BatchAccountResourceInformation batchAccountInformation, AzureCloudConfig azureCloudConfig, AzureServicesConnectionStringCredentialOptions credentialOptions)
        {
            ArgumentNullException.ThrowIfNull(batchAccountInformation);
            ArgumentNullException.ThrowIfNull(azureCloudConfig);
            ArgumentNullException.ThrowIfNull(credentialOptions);

            if (string.IsNullOrEmpty(batchAccountInformation.SubscriptionId))
            {
                throw new ArgumentException("Batch account information does not contain the subscription id. ", nameof(batchAccountInformation));
            }

            if (string.IsNullOrEmpty(batchAccountInformation.ResourceGroupName))
            {
                throw new ArgumentException("Batch account information does not contain the resource group name.", nameof(batchAccountInformation));
            }

            credentialOptions.AuthorityHost = azureCloudConfig.AuthorityHost;

            this.batchAccountInformation = batchAccountInformation;
            this.azureCloudConfig = azureCloudConfig;
            this.credentialOptions = credentialOptions;
        }

        /// <summary>
        /// Protected constructor.
        /// </summary>
        protected AzureManagementClientsFactory() { }

        /// <summary>
        /// Creates Batch Account management client using AAD authentication.
        /// Configure to the subscription id that contains the batch account.
        /// </summary>
        /// <returns></returns>
        public BatchAccountResource CreateBatchAccountManagementClient()
            => new ArmClient(
                    new AzureServicesConnectionStringCredential(credentialOptions),
                    batchAccountInformation.SubscriptionId,
                    new() { Environment = azureCloudConfig.ArmEnvironment })
            .GetBatchAccountResource(BatchAccountResource.CreateResourceIdentifier(batchAccountInformation.SubscriptionId, batchAccountInformation.ResourceGroupName, batchAccountInformation.Name));
    }
}
