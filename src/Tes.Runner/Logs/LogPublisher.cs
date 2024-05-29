// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Sas;
using Tes.Runner.Models;
using Tes.Runner.Storage;

namespace Tes.Runner.Logs
{
    public class LogPublisher(Func<ConsoleStreamLogPublisher> consoleStreamLogPublisherFactory, Func<Uri, string, AppendBlobLogPublisher> appendBlobLogPublisherFactory, UrlTransformationStrategyFactory transformationStrategyFactory)
    {
        const BlobSasPermissions LogLocationPermissions = BlobSasPermissions.Read | BlobSasPermissions.Create | BlobSasPermissions.Write | BlobSasPermissions.Add;

        private readonly Func<ConsoleStreamLogPublisher> consoleStreamLogPublisherFactory = consoleStreamLogPublisherFactory ?? throw new ArgumentNullException(nameof(consoleStreamLogPublisherFactory));
        private readonly Func<Uri, string, AppendBlobLogPublisher> appendBlobLogPublisherFactory = appendBlobLogPublisherFactory ?? throw new ArgumentNullException(nameof(appendBlobLogPublisherFactory));

        public async Task<IStreamLogReader> CreateStreamReaderLogPublisherAsync(NodeTask nodeTask, string logNamePrefix, string apiVersion)
        {
            ArgumentNullException.ThrowIfNull(nodeTask);
            ArgumentException.ThrowIfNullOrEmpty(logNamePrefix);

            if (!string.IsNullOrWhiteSpace(nodeTask.RuntimeOptions?.StreamingLogPublisher?.TargetUrl))
            {
                var transformedUrl = await transformationStrategyFactory.GetTransformedUrlAsync(
                    nodeTask.RuntimeOptions,
                    nodeTask.RuntimeOptions.StreamingLogPublisher,
                    LogLocationPermissions);

                return appendBlobLogPublisherFactory(
                    transformedUrl,
                    logNamePrefix);
            }

            return consoleStreamLogPublisherFactory();
        }
    }
}
