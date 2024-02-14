// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Options
{
    /// <summary>
    /// Options to configure the Azure cloud
    /// </summary>
    public class ArmEnvironmentOptions
    {
        /// <summary>
        /// Configuration section.
        /// </summary>
        public const string SectionName = "Arm";

        /// <summary>
        /// Name of Azure cloud
        /// </summary>
        /// <remarks>
        /// This is exoected to be one of the following values: <c>AzurePublicCloud</c>, <c>AzureUSGovernmentCloud</c>, or <c>AzureChinaCloud</c>.
        /// See https://learn.microsoft.com/en-us/azure/virtual-machines/instance-metadata-service?tabs=linux#sample-5-get-the-azure-environment-where-the-vm-is-running for details.
        /// </remarks>
        public string Name { get; set; }

        /// <summary>
        /// Azure Resource Manager (ARM) Endpoint
        /// </summary>
        public string Endpoint { get; set; }
    }
}
