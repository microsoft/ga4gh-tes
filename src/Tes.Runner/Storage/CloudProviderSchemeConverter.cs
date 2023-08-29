// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Sas;

namespace Tes.Runner.Storage
{
    public class CloudProviderSchemeConverter : ISasResolutionStrategy
    {
        private const string GcpHost = "storage.googleapis.com";
        private const string AwsSuffix = ".s3.amazonaws.com";

        public Task<Uri> CreateSasTokenWithStrategyAsync(string sourceUrl, BlobSasPermissions blobSasPermissions)
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

        private Uri ToGcpHttpUri(Uri sourceUri)
        {
            var builder = new UriBuilder();
            builder.Scheme = "https";

            builder.Host = GcpHost;
            builder.Path = $"{sourceUri.Host}{sourceUri.AbsolutePath}";
            builder.Query = sourceUri.Query;

            return builder.Uri;
        }

        private Uri ToAwsS3HttpUri(Uri sourceUri)
        {
            var builder = new UriBuilder();
            builder.Scheme = "https";

            // Assume the host is the S3 bucket name
            builder.Host = $"{sourceUri.Host}{AwsSuffix}";
            builder.Path = sourceUri.AbsolutePath;
            builder.Query = sourceUri.Query;

            return builder.Uri;
        }
    }
}
