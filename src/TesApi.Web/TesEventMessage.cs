// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using TesApi.Web.Storage;

namespace TesApi.Web
{
    /// <summary>
    /// Represents the events sent by the node task runner.
    /// </summary>
    public class TesEventMessage
    {
        static TesEventMessage() => Tes.Utilities.NewtonsoftJsonSafeInit.SetDefaultSettings();

        /// <summary>
        /// Blob tag used to record event processing.
        /// </summary>
        public const string ProcessedTag = "processed";

        private readonly IStorageAccessProvider _storageAccessProvider;
        private readonly IAzureProxy _azureProxy;
        private readonly ILogger _logger;
        private readonly Uri _uri;

        /// <summary>
        /// Tags of this event message.
        /// </summary>
        public IDictionary<string, string> Tags { get; }

        /// <summary>
        /// Event of this event message.
        /// </summary>
        public string Event { get; }

        /// <summary>
        /// Constructor of <see cref="TesEventMessage"/>.
        /// </summary>
        /// <param name="azureProxy"></param>
        /// <param name="logger"></param>
        /// <param name="storageAccessProvider"></param>
        /// <param name="blobAbsoluteUri"></param>
        /// <param name="tags"></param>
        /// <param name="event"></param>
        public TesEventMessage(IAzureProxy azureProxy, ILogger<TesEventMessage> logger, IStorageAccessProvider storageAccessProvider, Uri blobAbsoluteUri, IDictionary<string, string> tags, string @event)
        {
            ArgumentNullException.ThrowIfNull(azureProxy);
            ArgumentNullException.ThrowIfNull(storageAccessProvider);
            ArgumentNullException.ThrowIfNull(blobAbsoluteUri);
            ArgumentNullException.ThrowIfNull(tags);
            ArgumentNullException.ThrowIfNull(@event);

            if (tags.ContainsKey(ProcessedTag))
            {
                throw new ArgumentException("This message was already processed.", nameof(tags));
            }

            _azureProxy = azureProxy;
            _logger = logger;
            _storageAccessProvider = storageAccessProvider;
            _uri = blobAbsoluteUri;
            Tags = tags.AsReadOnly();
            Event = @event;
        }

        /// <summary>
        /// Gets the details of this event message.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<Tes.Runner.Events.EventMessage> GetMessageAsync(CancellationToken cancellationToken)
        {
            var messageText = await _azureProxy.DownloadBlobAsync(_uri, cancellationToken);
            var result = Newtonsoft.Json.JsonConvert.DeserializeObject<Tes.Runner.Events.EventMessage>(messageText);
            // TODO: throw if null
            return result;
        }

        /// <summary>
        /// Marks this event message processed.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task MarkMessageProcessed(CancellationToken cancellationToken)
        {
            var uri = await _storageAccessProvider.MapLocalPathToSasUrlAsync(_uri.ToString(), cancellationToken);
            await _azureProxy.SetBlobTags(new Uri(uri), Tags.Append(new KeyValuePair<string, string>(ProcessedTag, DateTime.UtcNow.ToString("O"))).ToDictionary(pair => pair.Key, pair => pair.Value), cancellationToken);
        }
    }

    /// <summary>
    /// Factory to create TesEventMessage instances.
    /// </summary>
    public sealed class BatchTesEventMessageFactory
    {
        private readonly IServiceProvider _serviceProvider;

        /// <summary>
        /// Constructor for <see cref="BatchPoolFactory"/>.
        /// </summary>
        /// <param name="serviceProvider">A service object.</param>
        public BatchTesEventMessageFactory(IServiceProvider serviceProvider) => _serviceProvider = serviceProvider;

        /// <summary>
        /// Creates a new <see cref="TesEventMessage"/>.
        /// </summary>
        /// <param name="blobAbsoluteUri"></param>
        /// <param name="tags"></param>
        /// <param name="event"></param>
        /// <returns></returns>
        public TesEventMessage CreateNew(Uri blobAbsoluteUri, IDictionary<string, string> tags, string @event)
            => ActivatorUtilities.CreateInstance<TesEventMessage>(_serviceProvider, blobAbsoluteUri, tags, @event);
    }
}
