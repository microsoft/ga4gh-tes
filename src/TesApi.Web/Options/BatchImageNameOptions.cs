// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Web.Options
{
    /// <summary>
    /// Batch compute node image name configuration options
    /// </summary>
    /// <remarks>
    /// CromwellDrsLocalizer is defined in <see cref="MarthaOptions.CromwellDrsLocalizer"/>.
    /// </remarks>
    public class BatchImageNameOptions
    {
        /// <summary>
        /// Batch compute node image name configuration section
        /// </summary>
        public const string SectionName = "NodeImages";

        /// <summary>
        /// Default value for <see cref="Docker"/>
        /// </summary>
        public const string DefaultDocker = "docker";

        /// <summary>
        /// Name of the docker image
        /// </summary>
        public string Docker { get; set; } = DefaultDocker;
    }
}
