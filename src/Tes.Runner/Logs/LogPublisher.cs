// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Sas;
using Tes.Runner.Models;
using Tes.Runner.Storage;

namespace Tes.Runner.Logs
{
    public class LogPublisher
    {
        const BlobSasPermissions LogLocationPermissions = BlobSasPermissions.Read | BlobSasPermissions.Create | BlobSasPermissions.Write | BlobSasPermissions.Add;

        public static async Task<IStreamLogReader> CreateStreamReaderLogPublisherAsync(RuntimeOptions runtimeOptions, string logNamePrefix, string apiVersion)
        {
            ArgumentNullException.ThrowIfNull(runtimeOptions);
            ArgumentException.ThrowIfNullOrEmpty(logNamePrefix);

            if (!string.IsNullOrWhiteSpace(runtimeOptions?.StreamingLogPublisher?.TargetUrl))
            {
                var transformedUrl = await UrlTransformationStrategyFactory.GetTransformedUrlAsync(
                    runtimeOptions,
                    runtimeOptions.StreamingLogPublisher,
                    LogLocationPermissions,
                    apiVersion);

                return new AppendBlobLogPublisher(
                    transformedUrl.ToString()!,
                    logNamePrefix);
            }

            return new ConsoleStreamLogPublisher();
        }
    }
}
