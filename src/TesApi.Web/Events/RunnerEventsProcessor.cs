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
        /// <param name="azureProxy"></param>
        /// <param name="storageAccessProvider"></param>
        /// <param name="logger"></param>
        public RunnerEventsProcessor(IAzureProxy azureProxy, Storage.IStorageAccessProvider storageAccessProvider, ILogger<RunnerEventsProcessor> logger)
        {
            ArgumentNullException.ThrowIfNull(azureProxy);
            ArgumentNullException.ThrowIfNull(storageAccessProvider);

            this.azureProxy = azureProxy;
            this.storageAccessProvider = storageAccessProvider;
            this.logger = logger;
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

            if (!message.Tags.ContainsKey("event-name") || !message.Tags.ContainsKey("task-id") || !message.Tags.ContainsKey("created"))
            {
                throw new ArgumentException("This message is missing needed tags.", nameof(message));
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
            ArgumentNullException.ThrowIfNull(message);

            Tes.Runner.Events.EventMessage result;

            try
            {
                var messageText = await azureProxy.DownloadBlobAsync(message.BlobUri, cancellationToken);
                result = System.Text.Json.JsonSerializer.Deserialize<Tes.Runner.Events.EventMessage>(messageText)
                    ?? throw new InvalidOperationException("Deserialize() returned null.");
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Event message blob is malformed. {ex.GetType().FullName}:{ex.Message}", ex);
            }

            message.SetRunnerEventMessage(result);

            // Validate content
            Assert(Guid.TryParse(result.Id, out _),
                $"{nameof(result.Id)}('{result.Id}')  is malformed.");
            Assert(Tes.Runner.Events.EventsPublisher.EventVersion.Equals(result.EventVersion, StringComparison.Ordinal),
                $"{nameof(result.EventVersion)}('{result.EventVersion}')  is not recognized.");
            Assert(Tes.Runner.Events.EventsPublisher.EventDataVersion.Equals(result.EventDataVersion, StringComparison.Ordinal),
                $"{nameof(result.EventDataVersion)}('{result.EventDataVersion}')  is not recognized.");
            Assert(Tes.Runner.Events.EventsPublisher.TesTaskRunnerEntityType.Equals(result.EntityType, StringComparison.Ordinal),
                $"{nameof(result.EntityType)}('{result.EntityType}')  is not recognized.");

            Assert(message.TesTaskId.Equals(result.EntityId, StringComparison.Ordinal),
                $"{nameof(result.EntityId)}('{result.EntityId}') does not match the expected value of '{message.TesTaskId}'.");
            Assert(result.EntityId.Equals(message.Tags["task-id"], StringComparison.Ordinal),
                $"{nameof(result.Name)}('{result.EntityId}') does not match the expected value of '{message.Tags["task-id"]}' from the tags..");
            Assert(message.Event.Equals(result.Name, StringComparison.OrdinalIgnoreCase),
                $"{nameof(result.Name)}('{result.Name}') does not match the expected value of '{message.Event}' from the blob path.");
            Assert(result.Name.Equals(message.Tags["event-name"], StringComparison.Ordinal),
                $"{nameof(result.Name)}('{result.Name}') does not match the expected value of '{message.Tags["event-name"]}' from the tags.");

            // Event type specific validations
            switch (result.Name)
            {
                case Tes.Runner.Events.EventsPublisher.DownloadStartEvent:
                    Assert(Tes.Runner.Events.EventsPublisher.StartedStatus.Equals(result.StatusMessage, StringComparison.Ordinal),
                        $"{nameof(result.StatusMessage)}('{result.StatusMessage}') does not match the expected value of '{Tes.Runner.Events.EventsPublisher.StartedStatus}'.");
                    break;

                case Tes.Runner.Events.EventsPublisher.DownloadEndEvent:
                    Assert(new[] { Tes.Runner.Events.EventsPublisher.SuccessStatus, Tes.Runner.Events.EventsPublisher.FailedStatus }.Contains(result.StatusMessage),
                        $"{nameof(result.StatusMessage)}('{result.StatusMessage}') does not match one of the expected valued of '{Tes.Runner.Events.EventsPublisher.SuccessStatus}' or '{Tes.Runner.Events.EventsPublisher.FailedStatus}'.");
                    break;

                case Tes.Runner.Events.EventsPublisher.UploadStartEvent:
                    Assert(Tes.Runner.Events.EventsPublisher.StartedStatus.Equals(result.StatusMessage, StringComparison.Ordinal),
                        $"{nameof(result.StatusMessage)}('{result.StatusMessage}') does not match the expected value of '{Tes.Runner.Events.EventsPublisher.StartedStatus}'.");
                    break;

                case Tes.Runner.Events.EventsPublisher.UploadEndEvent:
                    Assert(new[] { Tes.Runner.Events.EventsPublisher.SuccessStatus, Tes.Runner.Events.EventsPublisher.FailedStatus }.Contains(result.StatusMessage),
                        $"{nameof(result.StatusMessage)}('{result.StatusMessage}') does not match one of the expected valued of '{Tes.Runner.Events.EventsPublisher.SuccessStatus}' or '{Tes.Runner.Events.EventsPublisher.FailedStatus}'.");
                    break;

                case Tes.Runner.Events.EventsPublisher.ExecutorStartEvent:
                    Assert(Tes.Runner.Events.EventsPublisher.StartedStatus.Equals(result.StatusMessage, StringComparison.Ordinal),
                        $"{nameof(result.StatusMessage)}('{result.StatusMessage}') does not match the expected value of '{Tes.Runner.Events.EventsPublisher.StartedStatus}'.");
                    break;

                case Tes.Runner.Events.EventsPublisher.ExecutorEndEvent:
                    Assert(new[] { Tes.Runner.Events.EventsPublisher.SuccessStatus, Tes.Runner.Events.EventsPublisher.FailedStatus }.Contains(result.StatusMessage),
                        $"{nameof(result.StatusMessage)}('{result.StatusMessage}') does not match one of the expected valued of '{Tes.Runner.Events.EventsPublisher.SuccessStatus}' or '{Tes.Runner.Events.EventsPublisher.FailedStatus}'.");
                    break;

                case Tes.Runner.Events.EventsPublisher.TaskCompletionEvent:
                    Assert(new[] { Tes.Runner.Events.EventsPublisher.SuccessStatus, Tes.Runner.Events.EventsPublisher.FailedStatus }.Contains(result.StatusMessage),
                        $"{nameof(result.StatusMessage)}('{result.StatusMessage}') does not match one of the expected valued of '{Tes.Runner.Events.EventsPublisher.SuccessStatus}' or '{Tes.Runner.Events.EventsPublisher.FailedStatus}'.");
                    break;

                default:
                    Assert(false, $"{nameof(result.Name)}('{result.Name}') is not recognized.");
                    break;
            }

            static void Assert([System.Diagnostics.CodeAnalysis.DoesNotReturnIf(false)] bool condition, string message)
            {
                if (!condition)
                {
                    throw new InvalidOperationException(message);
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
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="messageGetter"></param>
        /// <returns></returns>
        public IEnumerable<T> OrderProcessedByExecutorSequence<T>(IEnumerable<T> source, Func<T, RunnerEventsMessage> messageGetter)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(messageGetter);

            return source.OrderBy(t => OrderBy(messageGetter(t))).ThenBy(t => ThenBy(messageGetter(t)));

            static DateTime OrderBy(RunnerEventsMessage message)
                => message.RunnerEventMessage?.Created ?? DateTime.Parse(message.Tags["created"]).ToUniversalTime();

            static int ThenBy(RunnerEventsMessage message)
                => ParseEventName(message.RunnerEventMessage is null
                    ? message.Tags["event-name"]
                    : message.RunnerEventMessage.Name);

            static int ParseEventName(string eventName)
                => EventsInOrder.TryGetValue(eventName, out var result) ? result : int.MinValue;
        }

        /// <summary>
        /// Gets the task status details from this event message.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="tesTask"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<AzureBatchTaskState> GetMessageBatchStateAsync(RunnerEventsMessage message, Tes.Models.TesTask tesTask, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(message);
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

            var processLogs = await GetProcessLogs(nodeMessage, tesTask, cancellationToken).ToListAsync(cancellationToken);

            if (processLogs.Any())
            {
                processLogs.Insert(0, "Possibly relevant logs:");
                state.Failure?.AppendRangeToSystemLogs(processLogs);
            }

            return state;

            // Helpers
            static IEnumerable<AzureBatchTaskState.OutputFileLog> GetFileLogs(IDictionary<string, string> eventData)
            {
                const string marker = "/wd/";

                if (eventData is null)
                {
                    yield break;
                }

                var numberOfFiles = int.Parse(eventData["numberOfFiles"]);
                for (var i = 0; i < numberOfFiles; ++i)
                {
                    var nodePath = eventData[$"filePath-{i}"];
                    var idxStart = nodePath.IndexOf(marker);

                    if (idxStart > 0)
                    {
                        var containerPathUnderRoot = nodePath[(idxStart + marker.Length)..];
                        var idxDirectory = containerPathUnderRoot.IndexOf('/');

                        if (idxDirectory > 0)
                        {
                            yield return new(
                            new Azure.Storage.Blobs.BlobUriBuilder(new Uri(eventData[$"fileUri-{i}"])) { Sas = null, Query = null }.ToUri(),
                            $"/{containerPathUnderRoot}",
                            long.Parse(eventData[$"fileSize-{i}"]));
                        }
                    }
                }
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

                var listUri = await storageAccessProvider.GetInternalTesTaskBlobUrlAsync(tesTask, string.Empty, Azure.Storage.Sas.BlobSasPermissions.List, cancellationToken);

                await foreach (var uri in azureProxy.ListBlobsAsync(new(listUri), cancellationToken)
                    .Where(blob => blob.BlobName.EndsWith(".txt") && System.IO.Path.GetFileName(blob.BlobName).StartsWith(blobNameStartsWith))
                    .OrderBy(blob => blob.BlobName)
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
        /// <param name="message"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task MarkMessageProcessedAsync(RunnerEventsMessage message, CancellationToken cancellationToken)
        {
            await azureProxy.SetBlobTags(
                message.BlobUri,
                message.Tags
                    .Append(new(ProcessedTag, DateTime.UtcNow.ToString("O")))
                    .ToDictionary(pair => pair.Key, pair => pair.Value),
                cancellationToken);
        }
    }
}
