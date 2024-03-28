// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;

namespace TesApi.Web.Options
{
    /// <summary>
    /// Service deployment configuration options
    /// </summary>
    public class DeploymentOptions
    {
        /// <summary>
        /// Service deployment configuration section
        /// </summary>
        public const string SectionName = "Deployment";

        /// <summary>
        /// Name of the organization responsible for the service.
        /// </summary>
        public string OrganizationName { get; set; }

        /// <summary>
        /// URL of the website of the organization.
        /// </summary>
        public string OrganizationUrl { get; set; }

        /// <summary>
        /// Contact url.
        /// </summary>
        public string ContactUri { get; set; }

        /// <summary>
        /// Environment the service is running in. Use this to distinguish between production, development and testing/staging deployments. Suggested values are prod, test, dev, staging. However this is advised and not enforced.
        /// </summary>
        public string Environment { get; set; }

        /// <summary>
        /// Timestamp describing when the service was first deployed and available.
        /// </summary>
        public DateTimeOffset Created { get; set; }

        /// <summary>
        /// Timestamp describing when the service was last updated.
        /// </summary>
        public DateTimeOffset Updated { get; set; }

        /// <summary>
        /// Image of the deployed service
        /// </summary>
        public string TesImage { get; set; }

        /// <summary>
        /// TES configured hostname.
        /// </summary>
        public string TesHostname { get; set; }

        /// <summary>
        /// TES configured LetsEncrypt email
        /// </summary>
        public string LetsEncryptEmail { get; set; }
    }
}
