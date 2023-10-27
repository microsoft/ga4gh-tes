// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace TesApi.Web.Events
{
    /// <summary>
    /// Represents an event sent by the node task runner.
    /// </summary>
    public class RunnerEventsProcessor
    {
        /// <summary>
        /// Blob tag used to record event processing.
        /// </summary>
        public const string ProcessedTag = "processed";

        private readonly IAzureProxy _azureProxy;
        private readonly ILogger _logger;

        /// <summary>
        /// Constructor of <see cref="RunnerEventsProcessor"/>.
        /// </summary>
        /// <param name="azureProxy"></param>
        /// <param name="logger"></param>
        public RunnerEventsProcessor(IAzureProxy azureProxy, ILogger<RunnerEventsProcessor> logger)
        {
            ArgumentNullException.ThrowIfNull(azureProxy);

            _azureProxy = azureProxy;
            _logger = logger;
        }


        /// <summary>
        /// TODO
        /// </summary>
        /// <param name="message"></param>
        /// <exception cref="ArgumentException"></exception>
        public void ValidateMessageMetadata(RunnerEventsMessage message)
        {
            ArgumentNullException.ThrowIfNull(message);

            if (message.BlobUri is null)
            {
                throw new ArgumentException("This message's URL is missing.", nameof(message));
            }

            if (message.Tags is null)
            {
                throw new ArgumentException("This message's Tags are missing.", nameof(message));
            }

            if (string.IsNullOrWhiteSpace(message.Event))
            {
                throw new ArgumentException("This message's event type is missing.", nameof(message));
            }

            if (message.Tags.Count == 0)
            {
                throw new ArgumentException("This message has no tags.", nameof(message));
            }

            if (message.Tags.ContainsKey(ProcessedTag))
            {
                throw new ArgumentException("This message was already processed.", nameof(message));
            }

            // There are up to 10 tags allowed. We will be adding one.
            // https://learn.microsoft.com/azure/storage/blobs/storage-manage-find-blobs?tabs=azure-portal#setting-blob-index-tags
            if (message.Tags.Count > 9)
            {
                throw new ArgumentException("This message does not have space to add the processed tag.", nameof(message));
            }
        }

        /// <summary>
        /// Gets the details of this event message.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task DownloadAndValidateMessageContentAsync(RunnerEventsMessage message, CancellationToken cancellationToken)
        {
            Tes.Runner.Events.EventMessage result;

            try
            {
                var messageText = await _azureProxy.DownloadBlobAsync(message.BlobUri, cancellationToken);
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
            System.Diagnostics.Debug.Assert(message.Event.Equals(result.Name, StringComparison.Ordinal));

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

            message.SetRunnerEventMessage(result);
        }

        private enum EventsInOrder
        {
            downloadStart,
            downloadEnd,
            executorStart,
            executorEnd,
            uploadStart,
            uploadEnd,
            taskCompleted,
        }

        /// <summary>
        /// Returns a sequence in the order the events were produced.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="messageGetter"></param>
        /// <returns></returns>
        public IEnumerable<T> OrderProcessedByExecutorSequence<T>(IEnumerable<T> source, Func<T, RunnerEventsMessage> messageGetter)
        {
            return source.OrderBy(t => messageGetter(t).RunnerEventMessage.Created).ThenBy(t => Enum.TryParse(typeof(EventsInOrder), messageGetter(t).RunnerEventMessage.Name, true, out var result) ? result : -1);
        }

        /// <summary>
        /// Gets the task status details from this event message.
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        public AzureBatchTaskState GetMessageBatchState(RunnerEventsMessage message)
        {
            ArgumentNullException.ThrowIfNull(message);
            ArgumentNullException.ThrowIfNull(message.RunnerEventMessage, nameof(message));

            var nodeMessage = message.RunnerEventMessage;

            _logger.LogDebug("Getting batch task state from event {EventName} for {TesTask}.", nodeMessage.Name ?? message.Event, nodeMessage.EntityId);
            return (nodeMessage.Name ?? message.Event) switch
            {
                Tes.Runner.Events.EventsPublisher.DownloadStartEvent => new(AzureBatchTaskState.TaskState.NoChange,
                    BatchTaskStartTime: nodeMessage.Created),

                Tes.Runner.Events.EventsPublisher.DownloadEndEvent => nodeMessage.StatusMessage switch
                {
                    Tes.Runner.Events.EventsPublisher.SuccessStatus => new(AzureBatchTaskState.TaskState.NoChange),

                    Tes.Runner.Events.EventsPublisher.FailedStatus => new(
                        AzureBatchTaskState.TaskState.NodeFilesUploadOrDownloadFailed,
                        Failure: new("SystemError",
                        Enumerable.Empty<string>()
                            .Append("Download failed.")
                            .Append(nodeMessage.EventData["errorMessage"]))),

                    _ => throw new System.Diagnostics.UnreachableException(),
                },

                Tes.Runner.Events.EventsPublisher.ExecutorStartEvent => new(AzureBatchTaskState.TaskState.Running,
                    ExecutorStartTime: nodeMessage.Created),

                Tes.Runner.Events.EventsPublisher.ExecutorEndEvent => nodeMessage.StatusMessage switch
                {
                    Tes.Runner.Events.EventsPublisher.SuccessStatus => new(
                        AzureBatchTaskState.TaskState.InfoUpdate,
                        ExecutorEndTime: nodeMessage.Created,
                        ExecutorExitCode: int.Parse(nodeMessage.EventData["exitCode"])),

                    Tes.Runner.Events.EventsPublisher.FailedStatus => new(
                        AzureBatchTaskState.TaskState.InfoUpdate,
                        Failure: new("ExecutorError",
                        Enumerable.Empty<string>()
                            .Append(nodeMessage.EventData["errorMessage"])),
                        ExecutorEndTime: nodeMessage.Created,
                        ExecutorExitCode: int.Parse(nodeMessage.EventData["exitCode"])),

                    _ => throw new System.Diagnostics.UnreachableException(),
                },

                Tes.Runner.Events.EventsPublisher.UploadStartEvent => new(AzureBatchTaskState.TaskState.NoChange),

                Tes.Runner.Events.EventsPublisher.UploadEndEvent => nodeMessage.StatusMessage switch
                {
                    Tes.Runner.Events.EventsPublisher.SuccessStatus => new(
                        AzureBatchTaskState.TaskState.InfoUpdate,
                        OutputFileLogs: GetFileLogs(nodeMessage.EventData)),

                    Tes.Runner.Events.EventsPublisher.FailedStatus => new(
                        AzureBatchTaskState.TaskState.NodeFilesUploadOrDownloadFailed,
                        Failure: new("SystemError",
                        Enumerable.Empty<string>()
                            .Append("Upload failed.")
                            .Append(nodeMessage.EventData["errorMessage"]))),

                    _ => throw new System.Diagnostics.UnreachableException(),
                },

                Tes.Runner.Events.EventsPublisher.TaskCompletionEvent => nodeMessage.StatusMessage switch
                {
                    Tes.Runner.Events.EventsPublisher.SuccessStatus => new(
                        AzureBatchTaskState.TaskState.CompletedSuccessfully,
                        BatchTaskStartTime: nodeMessage.Created - TimeSpan.Parse(nodeMessage.EventData["duration"]),
                        BatchTaskEndTime: nodeMessage.Created),

                    Tes.Runner.Events.EventsPublisher.FailedStatus => new(
                        AzureBatchTaskState.TaskState.CompletedWithErrors,
                        Failure: new("SystemError",
                        Enumerable.Empty<string>()
                            .Append("Node script failed.")
                            .Append(nodeMessage.EventData["errorMessage"])),
                        BatchTaskStartTime: nodeMessage.Created - TimeSpan.Parse(nodeMessage.EventData["duration"]),
                        BatchTaskEndTime: nodeMessage.Created),

                    _ => throw new System.Diagnostics.UnreachableException(),
                },

                _ => throw new System.Diagnostics.UnreachableException(),
            };

            static IEnumerable<AzureBatchTaskState.OutputFileLog> GetFileLogs(IDictionary<string, string> eventData)
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

        /// <summary>
        /// Marks this event message processed.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task MarkMessageProcessedAsync(RunnerEventsMessage message, CancellationToken cancellationToken)
        {
            await _azureProxy.SetBlobTags(
                message.BlobUri,
                message.Tags
                    .Append(new(ProcessedTag, DateTime.UtcNow.ToString("O")))
                    .ToDictionary(pair => pair.Key, pair => pair.Value),
                cancellationToken);
        }
    }
}
