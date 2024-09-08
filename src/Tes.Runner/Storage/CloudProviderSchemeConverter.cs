// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Sas;

namespace Tes.Runner.Storage
{
    public class CloudProviderSchemeConverter : IUrlTransformationStrategy
    {
        private const string GcpHost = "storage.googleapis.com";
        private const string AwsSuffix = ".s3.amazonaws.com";

        public Task<Uri> TransformUrlWithStrategyAsync(string sourceUrl, BlobSasPermissions blobSasPermissions)
        {
            var sourceUri = new Uri(sourceUrl);

            if (sourceUri.Scheme.Equals("gs", StringComparison.OrdinalIgnoreCase))
            {
                return Task.FromResult(ToGcpHttpUri(sourceUri));
            }

            if (sourceUri.Scheme.Equals("s3", StringComparison.OrdinalIgnoreCase))
            {
                return Task.FromResult(ToAwsS3HttpUri(sourceUri));
            }

            return Task.FromResult(sourceUri);
        }

        private static Uri ToGcpHttpUri(Uri sourceUri)
        {
            UriBuilder builder = new()
            {
                Scheme = "https",

                Host = GcpHost,
                Path = $"{sourceUri.Host}{sourceUri.AbsolutePath}",
                Query = sourceUri.Query
            };

            return builder.Uri;
        }

        private static Uri ToAwsS3HttpUri(Uri sourceUri)
        {
            UriBuilder builder = new()
            {
                Scheme = "https",

                // Assume the host is the S3 bucket name
                Host = $"{sourceUri.Host}{AwsSuffix}",
                Path = sourceUri.AbsolutePath,
                Query = sourceUri.Query
            };

            return builder.Uri;
        }
    }
}
