// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Sas;
using Microsoft.Extensions.Logging;
using Tes.Runner.Transfer;

namespace Tes.Runner.Storage
{
    internal class DrsTransformationStrategy : IUrlTransformationStrategy
    {
        private readonly ILogger<DrsTransformationStrategy> logger = PipelineLoggerFactory.Create<DrsTransformationStrategy>();
        private static DrsHubApiClient drsHubApiClient; 
        private const string DrsScheme = "drs://";


        private readonly Uri drsHubUri;

        public DrsTransformationStrategy(string drsHubUrl)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(drsHubUrl);
            drsHubApiClient = new DrsHubApiClient(drsHubUrl);

            drsHubUri = new Uri(drsHubUrl);
        }

        public async Task<Uri> TransformUrlWithStrategyAsync(string sourceUrl, BlobSasPermissions blobSasPermissions = 0)
        {
            var sourceUri = new Uri(sourceUrl);

            if(!ContainsDrsScheme(sourceUri.Scheme))
            {
                return sourceUri;
            }


        }

        private bool ContainsDrsScheme(string scheme)
        {
            return scheme.Equals(DrsScheme, StringComparison.OrdinalIgnoreCase);
        }
    }
}
