// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Options
{
    /// <summary>
    /// Martha configuration options
    /// </summary>
    public class MarthaOptions
    {
        /// <summary>
        /// Martha configuration section
        /// </summary>
        public const string SectionName = "Martha";
        private const string DefaultCromwellDrsLocalizer = "broadinstitute/cromwell-drs-localizer:develop";

        /// <summary>
        /// Name of the cromwell drs locator image
        /// </summary>
        public string CromwellDrsLocalizer { get; set; } = DefaultCromwellDrsLocalizer;
        /// <summary>
        /// MarthaKeyVaultName
        /// </summary>
        public string KeyVaultName { get; set; } = string.Empty;
        /// <summary>
        /// MarthaSecretName
        /// </summary>
        public string SecretName { get; set; } = string.Empty;
        /// <summary>
        /// MarthaUrl
        /// </summary>
        public string Url { get; set; } = string.Empty;
    }
}
