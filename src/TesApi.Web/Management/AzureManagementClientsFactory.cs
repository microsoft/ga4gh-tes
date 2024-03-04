// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using Azure.ResourceManager;
using Azure.ResourceManager.Batch;
using CommonUtilities;

namespace TesApi.Web.Management
{
    /// <summary>
    /// Factory of ARM management clients. 
    /// </summary>
    public class AzureManagementClientsFactory
    {
        private readonly BatchAccountResourceInformation batchAccountInformation;
        private readonly ArmEnvironmentEndpoints armEndpoints;
        private readonly AzureServicesConnectionStringCredentialOptions credentialOptions;

        /// <summary>
        /// Batch account resource information.
        /// </summary>
        public BatchAccountResourceInformation BatchAccountInformation => batchAccountInformation;

        /// <summary>
        /// Constructor of AzureManagementClientsFactory
        /// </summary>
        /// <param name="batchAccountInformation"><see cref="BatchAccountResourceInformation"/>></param>
        /// <param name="armEndpoints"></param>
        /// <param name="credentialOptions"></param>
        /// <exception cref="ArgumentException"></exception>
        public AzureManagementClientsFactory(BatchAccountResourceInformation batchAccountInformation, ArmEnvironmentEndpoints armEndpoints, AzureServicesConnectionStringCredentialOptions credentialOptions)
        {
            ArgumentNullException.ThrowIfNull(batchAccountInformation);
            ArgumentNullException.ThrowIfNull(armEndpoints);
            ArgumentNullException.ThrowIfNull(credentialOptions);

            if (string.IsNullOrEmpty(batchAccountInformation.SubscriptionId))
            {
                throw new ArgumentException("Batch account information does not contain the subscription id. ", nameof(batchAccountInformation));
            }

            if (string.IsNullOrEmpty(batchAccountInformation.ResourceGroupName))
            {
                throw new ArgumentException("Batch account information does not contain the resource group name.", nameof(batchAccountInformation));
            }

            credentialOptions.AuthorityHost = armEndpoints.AuthorityHost;

            this.batchAccountInformation = batchAccountInformation;
            this.armEndpoints = armEndpoints;
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
                    new() { Environment = new(armEndpoints.ResourceManager, armEndpoints.Audience) })
            .GetBatchAccountResource(BatchAccountResource.CreateResourceIdentifier(batchAccountInformation.SubscriptionId, batchAccountInformation.ResourceGroupName, batchAccountInformation.Name));
    }
}
