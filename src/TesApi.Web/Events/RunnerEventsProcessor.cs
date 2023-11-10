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

        private readonly IAzureProxy azureProxy;
        private readonly Storage.IStorageAccessProvider storageAccessProvider;
        private readonly ILogger logger;

        /// <summary>
        /// Constructor of <see cref="RunnerEventsProcessor"/>.
        /// </summary>
        /// <param name="azureProxy">Azure API wrapper.</param>
        /// <param name="storageAccessProvider">Methods for abstracting storage access.</param>
        /// <param name="logger">Methods for abstracting storage access.</param>
        public RunnerEventsProcessor(IAzureProxy azureProxy, Storage.IStorageAccessProvider storageAccessProvider, ILogger<RunnerEventsProcessor> logger)
        {
            ArgumentNullException.ThrowIfNull(azureProxy);
            ArgumentNullException.ThrowIfNull(storageAccessProvider);

            this.azureProxy = azureProxy;
            this.storageAccessProvider = storageAccessProvider;
            this.logger = logger;
        }


        /// <summary>
        /// Validate the <see cref="RunnerEventsMessage"/>.
        /// </summary>
        /// <param name="message">Tes runner event message metadata.</param>
        /// <exception cref="ArgumentException">Validation exceptions.</exception>
        public void ValidateMessageMetadata(RunnerEventsMessage message)
        {
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

            if (!message.Tags.ContainsKey("event-name") || !message.Tags.ContainsKey("task-id") || !message.Tags.ContainsKey("created"))
            {
                throw new ArgumentException("This message is missing required tags.", nameof(message));
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
        /// <param name="message">Tes runner event message metadata.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <exception cref="AssertException">Validation exceptions.</exception>
        /// <returns>A <see cref="RunnerEventsMessage"/> containing the associated <seealso cref="Tes.Runner.Events.EventMessage"/>.</returns>
        /// <remarks>This method assumes <paramref name="message"/> was successfully validated by <see cref="ValidateMessageMetadata(RunnerEventsMessage)"/>.</remarks>
        public async Task<RunnerEventsMessage> DownloadAndValidateMessageContentAsync(RunnerEventsMessage message, CancellationToken cancellationToken)
        {
            Tes.Runner.Events.EventMessage content;

            try
            {
                var messageText = await azureProxy.DownloadBlobAsync(message.BlobUri, cancellationToken);
                content = System.Text.Json.JsonSerializer.Deserialize<Tes.Runner.Events.EventMessage>(messageText)
                    ?? throw new InvalidOperationException("Deserialize() returned null.");
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Event message blob is malformed. {ex.GetType().FullName}:{ex.Message}", ex);
            }

            message = new(message, content);

            // Validate content
            Assert(Guid.TryParse(content.Id, out _),
                $"{nameof(content.Id)}('{content.Id}')  is malformed.");
            Assert(Tes.Runner.Events.EventsPublisher.EventVersion.Equals(content.EventVersion, StringComparison.Ordinal),
                $"{nameof(content.EventVersion)}('{content.EventVersion}')  is not recognized.");
            Assert(Tes.Runner.Events.EventsPublisher.EventDataVersion.Equals(content.EventDataVersion, StringComparison.Ordinal),
                $"{nameof(content.EventDataVersion)}('{content.EventDataVersion}')  is not recognized.");
            Assert(Tes.Runner.Events.EventsPublisher.TesTaskRunnerEntityType.Equals(content.EntityType, StringComparison.Ordinal),
                $"{nameof(content.EntityType)}('{content.EntityType}')  is not recognized.");

            Assert(message.TesTaskId.Equals(content.EntityId, StringComparison.Ordinal),
                $"{nameof(content.EntityId)}('{content.EntityId}') does not match the expected value of '{message.TesTaskId}'.");
            Assert(message.Tags["task-id"].Equals(content.EntityId, StringComparison.Ordinal),
                $"{nameof(content.EntityId)}('{content.EntityId}') does not match the expected value of '{message.Tags["task-id"]}' from the tags..");
            Assert(message.Event.Equals(content.Name, StringComparison.OrdinalIgnoreCase),
                $"{nameof(content.Name)}('{content.Name}') does not match the expected value of '{message.Event}' from the blob path.");
            Assert(message.Tags["event-name"].Equals(content.Name, StringComparison.Ordinal),
                $"{nameof(content.Name)}('{content.Name}') does not match the expected value of '{message.Tags["event-name"]}' from the tags.");

            // Event type specific validations
            switch (content.Name)
            {
                case Tes.Runner.Events.EventsPublisher.DownloadStartEvent:
                    Assert(Tes.Runner.Events.EventsPublisher.StartedStatus.Equals(content.StatusMessage, StringComparison.Ordinal),
                        $"{nameof(content.StatusMessage)}('{content.StatusMessage}') does not match the expected value of '{Tes.Runner.Events.EventsPublisher.StartedStatus}'.");
                    break;

                case Tes.Runner.Events.EventsPublisher.DownloadEndEvent:
                    Assert(new[] { Tes.Runner.Events.EventsPublisher.SuccessStatus, Tes.Runner.Events.EventsPublisher.FailedStatus }.Contains(content.StatusMessage),
                        $"{nameof(content.StatusMessage)}('{content.StatusMessage}') does not match one of the expected valued of '{Tes.Runner.Events.EventsPublisher.SuccessStatus}' or '{Tes.Runner.Events.EventsPublisher.FailedStatus}'.");
                    break;

                case Tes.Runner.Events.EventsPublisher.UploadStartEvent:
                    Assert(Tes.Runner.Events.EventsPublisher.StartedStatus.Equals(content.StatusMessage, StringComparison.Ordinal),
                        $"{nameof(content.StatusMessage)}('{content.StatusMessage}') does not match the expected value of '{Tes.Runner.Events.EventsPublisher.StartedStatus}'.");
                    break;

                case Tes.Runner.Events.EventsPublisher.UploadEndEvent:
                    Assert(new[] { Tes.Runner.Events.EventsPublisher.SuccessStatus, Tes.Runner.Events.EventsPublisher.FailedStatus }.Contains(content.StatusMessage),
                        $"{nameof(content.StatusMessage)}('{content.StatusMessage}') does not match one of the expected valued of '{Tes.Runner.Events.EventsPublisher.SuccessStatus}' or '{Tes.Runner.Events.EventsPublisher.FailedStatus}'.");
                    break;

                case Tes.Runner.Events.EventsPublisher.ExecutorStartEvent:
                    Assert(Tes.Runner.Events.EventsPublisher.StartedStatus.Equals(content.StatusMessage, StringComparison.Ordinal),
                        $"{nameof(content.StatusMessage)}('{content.StatusMessage}') does not match the expected value of '{Tes.Runner.Events.EventsPublisher.StartedStatus}'.");
                    break;

                case Tes.Runner.Events.EventsPublisher.ExecutorEndEvent:
                    Assert(new[] { Tes.Runner.Events.EventsPublisher.SuccessStatus, Tes.Runner.Events.EventsPublisher.FailedStatus }.Contains(content.StatusMessage),
                        $"{nameof(content.StatusMessage)}('{content.StatusMessage}') does not match one of the expected valued of '{Tes.Runner.Events.EventsPublisher.SuccessStatus}' or '{Tes.Runner.Events.EventsPublisher.FailedStatus}'.");
                    break;

                case Tes.Runner.Events.EventsPublisher.TaskCompletionEvent:
                    Assert(new[] { Tes.Runner.Events.EventsPublisher.SuccessStatus, Tes.Runner.Events.EventsPublisher.FailedStatus }.Contains(content.StatusMessage),
                        $"{nameof(content.StatusMessage)}('{content.StatusMessage}') does not match one of the expected valued of '{Tes.Runner.Events.EventsPublisher.SuccessStatus}' or '{Tes.Runner.Events.EventsPublisher.FailedStatus}'.");
                    break;

                default:
                    Assert(false, $"{nameof(content.Name)}('{content.Name}') is not recognized.");
                    break;
            }

            return message;

            static void Assert([System.Diagnostics.CodeAnalysis.DoesNotReturnIf(false)] bool condition, string message)
            {
                if (!condition)
                {
                    throw new AssertException(message);
                }
            }
        }

        private static readonly IReadOnlyDictionary<string, int> EventsInOrder = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
        {
            { Tes.Runner.Events.EventsPublisher.DownloadStartEvent, int.MinValue },
            { Tes.Runner.Events.EventsPublisher.DownloadEndEvent, int.MinValue + 1 },
            { Tes.Runner.Events.EventsPublisher.ExecutorStartEvent, -1 },
            { Tes.Runner.Events.EventsPublisher.ExecutorEndEvent, +1 },
            { Tes.Runner.Events.EventsPublisher.UploadStartEvent, int.MaxValue - 1 },
            { Tes.Runner.Events.EventsPublisher.UploadEndEvent, int.MaxValue },
        }.AsReadOnly();

        /// <summary>
        /// Returns a sequence in the order the events were produced.
        /// </summary>
        /// <typeparam name="T"><paramref name="source"/>'s enumerated type.</typeparam>
        /// <param name="source">Unordered enumeration of events.</param>
        /// <param name="messageGetter">Function that returns <see cref="RunnerEventsMessage"/> from <typeparamref name="T"/>.</param>
        /// <returns>Ordered enumeration of events.</returns>
        /// <remarks>This method assumes every <see cref="RunnerEventsMessage"/> was successfully validated by <see cref="ValidateMessageMetadata(RunnerEventsMessage)"/>.</remarks>
        public IEnumerable<T> OrderProcessedByExecutorSequence<T>(IEnumerable<T> source, Func<T, RunnerEventsMessage> messageGetter)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(messageGetter);

            return source.OrderBy(t => OrderBy(messageGetter(t))).ThenBy(t => ThenBy(messageGetter(t)));

            static DateTime OrderBy(RunnerEventsMessage message)
                => (message.RunnerEventMessage?.Created ?? DateTime.Parse(message.Tags["created"])).ToUniversalTime();

            static int ThenBy(RunnerEventsMessage message)
                => ParseEventName(message.RunnerEventMessage is null
                    ? message.Tags["event-name"]
                    : message.RunnerEventMessage.Name);

            static int ParseEventName(string eventName)
                => EventsInOrder.TryGetValue(eventName, out var result) ? result : 0;
        }

        /// <summary>
        /// Gets the task status details from this event message.
        /// </summary>
        /// <param name="message">Tes runner event message metadata.</param>
        /// <param name="tesTask"><see cref="Tes.Models.TesTask"/> associated with <paramref name="message"/>.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns><see cref="AzureBatchTaskState"/> populated from <paramref name="message"/>.</returns>
        /// <remarks>This method assumes <paramref name="message"/> was returned by <see cref="DownloadAndValidateMessageContentAsync"/>.</remarks>
        public async Task<AzureBatchTaskState> GetMessageBatchStateAsync(RunnerEventsMessage message, Tes.Models.TesTask tesTask, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(message.RunnerEventMessage, nameof(message));

            var nodeMessage = message.RunnerEventMessage;
            logger.LogDebug("Getting batch task state from event {EventName} for {TesTask}.", nodeMessage.Name ?? message.Event, nodeMessage.EntityId);

            var state = (nodeMessage.Name ?? message.Event) switch
            {
                Tes.Runner.Events.EventsPublisher.DownloadStartEvent => new AzureBatchTaskState(AzureBatchTaskState.TaskState.InfoUpdate,
                    BatchTaskStartTime: nodeMessage.Created),

                Tes.Runner.Events.EventsPublisher.DownloadEndEvent => nodeMessage.StatusMessage switch
                {
                    Tes.Runner.Events.EventsPublisher.SuccessStatus => new(AzureBatchTaskState.TaskState.NoChange),

                    Tes.Runner.Events.EventsPublisher.FailedStatus => new(
                        AzureBatchTaskState.TaskState.NodeFilesUploadOrDownloadFailed,
                        Failure: new("SystemError",
                        Enumerable.Empty<string>()
                            .Append("Download failed.")
                            .Append(nodeMessage.EventData["errorMessage"])
                            .Concat(await AddProcessLogsIfAvailable(nodeMessage, tesTask, cancellationToken)))),

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
                            .Append(nodeMessage.EventData["errorMessage"])
                            .Concat(await AddProcessLogsIfAvailable(nodeMessage, tesTask, cancellationToken))),
                        ExecutorEndTime: nodeMessage.Created,
                        ExecutorExitCode: int.Parse(nodeMessage.EventData["exitCode"])),

                    _ => throw new System.Diagnostics.UnreachableException(),
                },

                Tes.Runner.Events.EventsPublisher.UploadStartEvent => new(AzureBatchTaskState.TaskState.NoChange),

                Tes.Runner.Events.EventsPublisher.UploadEndEvent => nodeMessage.StatusMessage switch
                {
                    Tes.Runner.Events.EventsPublisher.SuccessStatus => new(
                        AzureBatchTaskState.TaskState.InfoUpdate,
                        OutputFileLogs: GetOutputFileLogs(nodeMessage.EventData)),

                    Tes.Runner.Events.EventsPublisher.FailedStatus => new(
                        AzureBatchTaskState.TaskState.NodeFilesUploadOrDownloadFailed,
                        Failure: new("SystemError",
                        Enumerable.Empty<string>()
                            .Append("Upload failed.")
                            .Append(nodeMessage.EventData["errorMessage"])
                            .Concat(await AddProcessLogsIfAvailable(nodeMessage, tesTask, cancellationToken)))),

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

            return state;

            // Helpers
            static IEnumerable<AzureBatchTaskState.OutputFileLog> GetOutputFileLogs(IDictionary<string, string> eventData)
            {
                if (eventData is null || !eventData.ContainsKey("fileLog-Count"))
                {
                    yield break;
                }

                var numberOfFiles = int.Parse(eventData["fileLog-Count"], System.Globalization.CultureInfo.InvariantCulture);

                for (var i = 0; i < numberOfFiles; ++i)
                {
                    yield return new(
                        new Uri(eventData[$"fileUri-{i}"]),
                        eventData[$"filePath-{i}"],
                        long.Parse(eventData[$"fileSize-{i}"], System.Globalization.CultureInfo.InvariantCulture));
                }
            }

            async ValueTask<IEnumerable<string>> AddProcessLogsIfAvailable(Tes.Runner.Events.EventMessage message, Tes.Models.TesTask tesTask, CancellationToken cancellationToken)
            {
                var processLogs = await GetProcessLogs(message, tesTask, cancellationToken).ToListAsync(cancellationToken);

                if (processLogs.Any())
                {
                    processLogs.Insert(0, "Possibly relevant logs:");
                }

                return processLogs;
            }

            async IAsyncEnumerable<string> GetProcessLogs(Tes.Runner.Events.EventMessage message, Tes.Models.TesTask tesTask, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
            {
                var blobNameStartsWith = message.Name switch
                {
                    Tes.Runner.Events.EventsPublisher.DownloadEndEvent => "download_std",
                    Tes.Runner.Events.EventsPublisher.ExecutorEndEvent => "exec_std",
                    Tes.Runner.Events.EventsPublisher.UploadEndEvent => "upload_std",
                    _ => string.Empty,
                };

                if (string.IsNullOrEmpty(blobNameStartsWith))
                {
                    yield break;
                }

                await foreach (var uri in azureProxy.ListBlobsAsync(new(await storageAccessProvider.GetInternalTesTaskBlobUrlAsync(tesTask, string.Empty, Azure.Storage.Sas.BlobSasPermissions.List, cancellationToken)), cancellationToken)
                    .Where(blob => blob.BlobName.EndsWith(".txt") && blob.BlobName.Split('/').Last().StartsWith(blobNameStartsWith))
                    .OrderBy(blob => blob.BlobName) // Not perfect ordering, but reasonable. This is more likely to be read by people rather then machines. Perfect would involve regex.
                    .Select(blob => blob.BlobUri)
                    .WithCancellation(cancellationToken))
                {
                    yield return uri.AbsoluteUri;
                }
            }
        }

        /// <summary>
        /// Marks this event message processed.
        /// </summary>
        /// <param name="message">Tes runner event message metadata.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        /// <remarks>This method assumes <paramref name="message"/> was successfully validated by <see cref="ValidateMessageMetadata(RunnerEventsMessage)"/>.</remarks>
        public async Task MarkMessageProcessedAsync(RunnerEventsMessage message, CancellationToken cancellationToken)
        {
            await azureProxy.SetBlobTags(
                message.BlobUri,
                message.Tags
                    .Append(new(ProcessedTag, DateTime.UtcNow.ToString("O")))
                    .ToDictionary(pair => pair.Key, pair => pair.Value),
                cancellationToken);
        }

        /// <summary>
        /// Validation assert failed.
        /// </summary>
        public class AssertException : InvalidOperationException
        {
            /// <inheritdoc/>
            public AssertException(string message) : base(message)
            {
            }
        }
    }
}
