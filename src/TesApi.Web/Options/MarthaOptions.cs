// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Options
{
    // TODO: elaborate here. Also document this feature
    /// <summary>
    /// Martha configuration options
    /// </summary>
    public class MarthaOptions
    {
        /// <summary>
        /// Martha configuration section
        /// </summary>
        public const string SectionName = "Martha";

        /// <summary>
        /// MarthaKeyVaultName
        /// </summary>
        public string KeyVaultName { get; set; }
        /// <summary>
        /// MarthaSecretName
        /// </summary>
        public string SecretName { get; set; }
        /// <summary>
        /// MarthaUrl
        /// </summary>
        public string Url { get; set; }
    }
}
