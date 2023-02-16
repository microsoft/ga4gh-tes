// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Options
{
    /// <summary>
    /// Batch compute node image name configuration options
    /// </summary>
    public class BatchImageNameOptions
    {
        /// <summary>
        /// Batch compute node image name configuration section
        /// </summary>
        public const string SectionName = "BatchNodes";

        /// <summary>
        /// Name of the blobxfer image
        /// </summary>
        public string Blobxfer { get; set; }
        /// <summary>
        /// Name of the docker image
        /// </summary>
        public string DockerInDocker { get; set; }
        /// <summary>
        /// Name of the cromwell drs locator image
        /// </summary>
        public string CromwellDrsLocalizer { get; set; }
    }
}
