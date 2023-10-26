// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using TesApi.Web.Storage;

namespace TesApi.Web.Events
{
    /// <summary>
    /// Represents an event sent by the node task runner.
    /// </summary>
    public class NodeEventMessage
    {
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
        /// Constructor of <see cref="NodeEventMessage"/>.
        /// </summary>
        /// <param name="azureProxy"></param>
        /// <param name="logger"></param>
        /// <param name="storageAccessProvider"></param>
        /// <param name="blobAbsoluteUri"></param>
        /// <param name="tags"></param>
        /// <param name="event"></param>
        public NodeEventMessage(IAzureProxy azureProxy, ILogger<NodeEventMessage> logger, IStorageAccessProvider storageAccessProvider, Uri blobAbsoluteUri, IDictionary<string, string> tags, string @event)
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
            Tes.Runner.Events.EventMessage result = null;

            try
            {
                var messageText = await _azureProxy.DownloadBlobAsync(_uri, cancellationToken);
                result = System.Text.Json.JsonSerializer.Deserialize<Tes.Runner.Events.EventMessage>(messageText)
                    ?? throw new InvalidOperationException("Deserialize() returned null.");
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Event message blob is malformed. {ex.GetType().FullName}:{ex.Message}", ex);
            }

            System.Diagnostics.Debug.Assert(Guid.TryParse(result.Id, out _));
            System.Diagnostics.Debug.Assert(Tes.Runner.Events.EventsPublisher.EventVersion.Equals(result.EventVersion, StringComparison.Ordinal));
            System.Diagnostics.Debug.Assert(Tes.Runner.Events.EventsPublisher.EventDataVersion.Equals(result.EventDataVersion, StringComparison.Ordinal));
            System.Diagnostics.Debug.Assert(Tes.Runner.Events.EventsPublisher.TesTaskRunnerEntityType.Equals(result.EntityType, StringComparison.Ordinal));
            System.Diagnostics.Debug.Assert(Event.Equals(result.Name, StringComparison.Ordinal));

            // Event type specific validations
            switch (result.Name)
            {
                case Tes.Runner.Events.EventsPublisher.DownloadStartEvent:
                    System.Diagnostics.Debug.Assert(Tes.Runner.Events.EventsPublisher.StartedStatus.Equals(result.StatusMessage, StringComparison.Ordinal));
                    break;

                case Tes.Runner.Events.EventsPublisher.DownloadEndEvent:
                    System.Diagnostics.Debug.Assert(new[] { Tes.Runner.Events.EventsPublisher.SuccessStatus, Tes.Runner.Events.EventsPublisher.FailedStatus }.Contains(result.StatusMessage));
                    break;

                case Tes.Runner.Events.EventsPublisher.UploadStartEvent:
                    System.Diagnostics.Debug.Assert(Tes.Runner.Events.EventsPublisher.StartedStatus.Equals(result.StatusMessage, StringComparison.Ordinal));
                    break;

                case Tes.Runner.Events.EventsPublisher.UploadEndEvent:
                    System.Diagnostics.Debug.Assert(new[] { Tes.Runner.Events.EventsPublisher.SuccessStatus, Tes.Runner.Events.EventsPublisher.FailedStatus }.Contains(result.StatusMessage));
                    break;

                case Tes.Runner.Events.EventsPublisher.ExecutorStartEvent:
                    System.Diagnostics.Debug.Assert(Tes.Runner.Events.EventsPublisher.StartedStatus.Equals(result.StatusMessage, StringComparison.Ordinal));
                    break;

                case Tes.Runner.Events.EventsPublisher.ExecutorEndEvent:
                    System.Diagnostics.Debug.Assert(new[] { Tes.Runner.Events.EventsPublisher.SuccessStatus, Tes.Runner.Events.EventsPublisher.FailedStatus }.Contains(result.StatusMessage));
                    break;

                case Tes.Runner.Events.EventsPublisher.TaskCompletionEvent:
                    System.Diagnostics.Debug.Assert(new[] { Tes.Runner.Events.EventsPublisher.SuccessStatus, Tes.Runner.Events.EventsPublisher.FailedStatus }.Contains(result.StatusMessage));
                    break;

                default:
                    System.Diagnostics.Debug.Assert(false);
                    break;
            }

            _logger.LogDebug("Getting batch task state from event {EventName} for {TesTask}.", result.Name ?? Event, result.EntityId);
            return (result.EntityId, GetBatchTaskState(result));
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
                Tags
                    .Append(new(ProcessedTag, DateTime.UtcNow.ToString("O")))
                    .ToDictionary(pair => pair.Key, pair => pair.Value),
                cancellationToken);
        }

        private AzureBatchTaskState GetBatchTaskState(Tes.Runner.Events.EventMessage message)
        {
            return (message.Name ?? Event) switch
            {
                Tes.Runner.Events.EventsPublisher.DownloadStartEvent => new(AzureBatchTaskState.TaskState.NoChange,
                    BatchTaskStartTime: message.Created),

                Tes.Runner.Events.EventsPublisher.DownloadEndEvent => message.StatusMessage switch
                {
                    Tes.Runner.Events.EventsPublisher.SuccessStatus => new(AzureBatchTaskState.TaskState.NoChange),

                    Tes.Runner.Events.EventsPublisher.FailedStatus => new(
                        AzureBatchTaskState.TaskState.NodeFilesUploadOrDownloadFailed,
                        Failure: new("SystemError",
                        Enumerable.Empty<string>()
                            .Append("Download failed.")
                            .Append(message.EventData["errorMessage"]))),

                    _ => throw new System.Diagnostics.UnreachableException(),
                },

                Tes.Runner.Events.EventsPublisher.ExecutorStartEvent => new(AzureBatchTaskState.TaskState.Running,
                    ExecutorStartTime: message.Created),

                Tes.Runner.Events.EventsPublisher.ExecutorEndEvent => message.StatusMessage switch
                {
                    Tes.Runner.Events.EventsPublisher.SuccessStatus => new(
                        AzureBatchTaskState.TaskState.InfoUpdate,
                        ExecutorEndTime: message.Created,
                        ExecutorExitCode: int.Parse(message.EventData["exitCode"])),

                    Tes.Runner.Events.EventsPublisher.FailedStatus => new(
                        AzureBatchTaskState.TaskState.InfoUpdate,
                        Failure: new("ExecutorError",
                        Enumerable.Empty<string>()
                            .Append(message.EventData["errorMessage"])),
                        ExecutorEndTime: message.Created,
                        ExecutorExitCode: int.Parse(message.EventData["exitCode"])),

                    _ => throw new System.Diagnostics.UnreachableException(),
                },

                Tes.Runner.Events.EventsPublisher.UploadStartEvent => new(AzureBatchTaskState.TaskState.NoChange),

                Tes.Runner.Events.EventsPublisher.UploadEndEvent => message.StatusMessage switch
                {
                    Tes.Runner.Events.EventsPublisher.SuccessStatus => new(
                        AzureBatchTaskState.TaskState.InfoUpdate,
                        OutputFileLogs: GetFileLogs(message.EventData)),

                    Tes.Runner.Events.EventsPublisher.FailedStatus => new(
                        AzureBatchTaskState.TaskState.NodeFilesUploadOrDownloadFailed,
                        Failure: new("SystemError",
                        Enumerable.Empty<string>()
                            .Append("Upload failed.")
                            .Append(message.EventData["errorMessage"]))),

                    _ => throw new System.Diagnostics.UnreachableException(),
                },

                Tes.Runner.Events.EventsPublisher.TaskCompletionEvent => message.StatusMessage switch
                {
                    Tes.Runner.Events.EventsPublisher.SuccessStatus => new(
                        AzureBatchTaskState.TaskState.CompletedSuccessfully,
                        BatchTaskStartTime: message.Created - TimeSpan.Parse(message.EventData["duration"]),
                        BatchTaskEndTime: message.Created),

                    Tes.Runner.Events.EventsPublisher.FailedStatus => new(
                        AzureBatchTaskState.TaskState.CompletedWithErrors,
                        Failure: new("SystemError",
                        Enumerable.Empty<string>()
                            .Append("Node script failed.")
                            .Append(message.EventData["errorMessage"])),
                        BatchTaskStartTime: message.Created - TimeSpan.Parse(message.EventData["duration"]),
                        BatchTaskEndTime: message.Created),

                    _ => throw new System.Diagnostics.UnreachableException(),
                },

                _ => throw new System.Diagnostics.UnreachableException(),
            };
        }

        private static IEnumerable<AzureBatchTaskState.OutputFileLog> GetFileLogs(IDictionary<string, string> eventData)
        {
            if (eventData is null)
            {
                yield break;
            }

            var numberOfFiles = int.Parse(eventData["numberOfFiles"]);
            for (var i = 0; i < numberOfFiles; ++i)
            {
                yield return new(
                    new Uri(eventData[$"fileUri-{i}"]),
                    eventData[$"filePath-{i}"],
                    long.Parse(eventData[$"fileSize-{i}"]));
            }
        }
    }
}
