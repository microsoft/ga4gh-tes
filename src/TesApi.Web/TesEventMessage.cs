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
    /// <remarks>This should be transient in DI.</remarks>
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

            if (tags.Count == 0)
            {
                throw new ArgumentException("This message has no tags.", nameof(tags));
            }

            if (tags.ContainsKey(ProcessedTag))
            {
                throw new ArgumentException("This message was already processed.", nameof(tags));
            }

            // There are up to 10 tags allowed. We will be adding one.
            // https://learn.microsoft.com/azure/storage/blobs/storage-manage-find-blobs?tabs=azure-portal#setting-blob-index-tags
            if (tags.Count > 9)
            {
                throw new ArgumentException("This message does not have space to add the processed tag.", nameof(tags));
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
        public async Task<(string Id, AzureBatchTaskState State)> GetMessageBatchStateAsync(CancellationToken cancellationToken)
        {
            var messageText = await _azureProxy.DownloadBlobAsync(_uri, cancellationToken);
            var result = Newtonsoft.Json.JsonConvert.DeserializeObject<Tes.Runner.Events.EventMessage>(messageText);

            // TODO: throw if null
            // Validate. Suggestions include:
            //Guid.TryParse(result.Id, out _)
            //Tes.Runner.Events.EventsPublisher.EventVersion.Equals(result.EventVersion, StringComparison.Ordinal)
            //Tes.Runner.Events.EventsPublisher.EventDataVersion.Equals(result.EventDataVersion, StringComparison.Ordinal)
            //Tes.Runner.Events.EventsPublisher.TesTaskRunnerEntityType.Equals(result.EntityType, StringComparison.Ordinal)
            //Tes.Runner.Events.EventsPublisher.TesTaskRunnerEntityType.Equals(result.EntityType, StringComparison.Ordinal)
            //Event.Equals(result.Name, StringComparison.Ordinal)
            //new[] { Tes.Runner.Events.EventsPublisher.StartedStatus, Tes.Runner.Events.EventsPublisher.SuccessStatus, Tes.Runner.Events.EventsPublisher.FailedStatus }.Contains(result.StatusMessage)

            // Event type specific validations
            //

            _logger.LogDebug("Getting batch task state from event {EventName} for {TesTask}.", result.Name ?? Event, result.EntityId);
            return (result.EntityId, GetCompletedBatchState(result));
        }

        /// <summary>
        /// Marks this event message processed.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task MarkMessageProcessed(CancellationToken cancellationToken)
        {
            await _azureProxy.SetBlobTags(
                _uri,
                Tags.Append(new(ProcessedTag, DateTime.UtcNow.ToString("O")))
                    .ToDictionary(pair => pair.Key, pair => pair.Value),
                cancellationToken);
        }

        private /*static*/ AzureBatchTaskState GetCompletedBatchState(Tes.Runner.Events.EventMessage task)
        {
            return (task.Name ?? Event) switch
            {
                Tes.Runner.Events.EventsPublisher.TaskCompletionEvent => string.IsNullOrWhiteSpace(task.EventData["errorMessage"])

                    ? new(
                        AzureBatchTaskState.TaskState.CompletedSuccessfully,
                        BatchTaskStartTime: task.Created - TimeSpan.Parse(task.EventData["duration"]),
                        BatchTaskEndTime: task.Created/*,
                            BatchTaskExitCode: 0*/)

                    : new(
                        AzureBatchTaskState.TaskState.CompletedWithErrors,
                        Failure: new("ExecutorError",
                        Enumerable.Empty<string>()
                            .Append(task.EventData["errorMessage"])),
                        BatchTaskStartTime: task.Created - TimeSpan.Parse(task.EventData["duration"]),
                        BatchTaskEndTime: task.Created/*,
                            BatchTaskExitCode: 0*/),

                Tes.Runner.Events.EventsPublisher.ExecutorStartEvent => new(AzureBatchTaskState.TaskState.Running),

                // TODO: the rest
                _ => new(AzureBatchTaskState.TaskState.NodePreempted), //throw new System.Diagnostics.UnreachableException(),
            };
        }
    }

    // TODO: Consider moving this class's implementation to Startup
    /// <summary>
    /// Factory to create TesEventMessage instances.
    /// </summary>
    /// <remarks>This can be a singleton in DI.</remarks>
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
