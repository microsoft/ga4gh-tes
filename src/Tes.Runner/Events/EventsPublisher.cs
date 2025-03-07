// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Sas;
using Microsoft.Extensions.Logging;
using Tes.Runner.Models;
using Tes.Runner.Storage;
using Tes.Runner.Transfer;

namespace Tes.Runner.Events;

public class EventsPublisher : IAsyncDisposable
{
    public static readonly Version EventVersion = new(1, 0);
    public static readonly Version EventDataVersion = new(1, 0);
    public const string TesTaskRunnerEntityType = "TesRunnerTask";
    public const string DownloadStartEvent = "downloadStart";
    public const string DownloadEndEvent = "downloadEnd";
    public const string UploadStartEvent = "uploadStart";
    public const string UploadEndEvent = "uploadEnd";
    public const string ExecutorStartEvent = "executorStart";
    public const string ExecutorEndEvent = "executorEnd";
    public const string TaskCommencementEvent = "taskStarted";
    public const string TaskCompletionEvent = "taskCompleted";

    private readonly IList<IEventSink> sinks;
    private readonly ILogger logger = PipelineLoggerFactory.Create<EventsPublisher>();

    public const string SuccessStatus = "Success";
    public const string FailedStatus = "Failed";
    public const string StartedStatus = "Started";

    public EventsPublisher(IList<IEventSink> sinks)
    {
        this.sinks = sinks;
    }

    /// <summary>
    /// Parameter-less constructor for mocking
    /// </summary>
    protected EventsPublisher()
    {
        this.sinks = [];
    }

    public static async Task<EventsPublisher> CreateEventsPublisherAsync(NodeTask nodeTask, string apiVersion)
    {
        var storageSink = await CreateAndStartStorageEventSinkFromTaskIfRequestedAsync(nodeTask, apiVersion);

        List<IEventSink> sinkList = [];
        if (storageSink != null)
        {
            sinkList.Add(storageSink);
        }

        return new EventsPublisher(sinkList);
    }

    private static async Task<IEventSink?> CreateAndStartStorageEventSinkFromTaskIfRequestedAsync(NodeTask nodeTask, string apiVersion)
    {
        ArgumentNullException.ThrowIfNull(nodeTask);

        if (nodeTask.RuntimeOptions.StorageEventSink is null)
        {
            return default;
        }

        if (string.IsNullOrWhiteSpace(nodeTask.RuntimeOptions.StorageEventSink.TargetUrl))
        {
            return default;
        }

        var transformationStrategy = UrlTransformationStrategyFactory.CreateStrategy(
            nodeTask.RuntimeOptions.StorageEventSink.TransformationStrategy,
            nodeTask.RuntimeOptions,
            apiVersion);


        var transformedUrl = await transformationStrategy.TransformUrlWithStrategyAsync(
            nodeTask.RuntimeOptions.StorageEventSink.TargetUrl,
            BlobSasPermissions.Write | BlobSasPermissions.Create | BlobSasPermissions.Tag);

        var sink = new BlobStorageEventSink(transformedUrl);

        sink.Start();

        return sink;
    }

    public virtual async Task PublishUploadStartEventAsync(NodeTask nodeTask)
    {
        var eventMessage = CreateNewEventMessage(nodeTask.Id, UploadStartEvent, StartedStatus,
            nodeTask.WorkflowId);

        await PublishAsync(eventMessage);
    }

    public virtual async Task PublishUploadEndEventAsync(NodeTask nodeTask, int numberOfFiles, long totalSizeInBytes, string statusMessage, string? errorMessage = default, IEnumerable<CompletedUploadFile>? completedFiles = default)
    {
        var eventMessage = CreateNewEventMessage(nodeTask.Id, UploadEndEvent, statusMessage,
            nodeTask.WorkflowId);

        eventMessage.EventData = new()
        {
            { "numberOfFiles", numberOfFiles.ToString()},
            { "totalSizeInBytes", totalSizeInBytes.ToString()},
            { "errorMessage", errorMessage ?? string.Empty}
        };

        if (completedFiles is not null)
        {
            completedFiles = completedFiles.ToList();
            eventMessage.EventData.Add(@"fileLog-Count", completedFiles.Count().ToString("D"));

            foreach (var (logEntry, index) in completedFiles.Select((logEntry, index) => (logEntry, index)))
            {
                eventMessage.EventData.Add($"fileSize-{index}", logEntry.Length.ToString("D"));
                eventMessage.EventData.Add($"fileUri-{index}", logEntry.BlobUrl?.AbsoluteUri ?? string.Empty);
                eventMessage.EventData.Add($"filePath-{index}", logEntry.FileName);
            }
        }

        await PublishAsync(eventMessage);
    }

    public virtual async Task PublishExecutorStartEventAsync(NodeTask nodeTask)
    {
        var eventMessage = CreateNewEventMessage(nodeTask.Id, ExecutorStartEvent, StartedStatus,
                       nodeTask.WorkflowId);

        var commands = nodeTask.CommandsToExecute ?? [];

        eventMessage.EventData = new()
        {
            { "image", nodeTask.ImageName??string.Empty},
            { "imageTag", nodeTask.ImageTag??string.Empty},
            { "commands", string.Join(' ', commands) }
        };
        await PublishAsync(eventMessage);
    }

    public virtual async Task PublishExecutorEndEventAsync(NodeTask nodeTask, long exitCode, string statusMessage, string? errorMessage = default)
    {
        var eventMessage = CreateNewEventMessage(nodeTask.Id, ExecutorEndEvent, statusMessage,
                                  nodeTask.WorkflowId);
        eventMessage.EventData = new()
        {
            { "image", nodeTask.ImageName??string.Empty},
            { "imageTag", nodeTask.ImageTag??string.Empty},
            { "exitCode", exitCode.ToString()},
            { "errorMessage", errorMessage??string.Empty}
        };
        await PublishAsync(eventMessage);
    }

    public virtual async Task PublishDownloadStartEventAsync(NodeTask nodeTask)
    {
        var eventMessage = CreateNewEventMessage(nodeTask.Id, DownloadStartEvent, StartedStatus,
                       nodeTask.WorkflowId);

        await PublishAsync(eventMessage);
    }

    public virtual async Task PublishDownloadEndEventAsync(NodeTask nodeTask, int numberOfFiles, long totalSizeInBytes, string statusMessage, string? errorMessage = default)
    {
        var eventMessage = CreateNewEventMessage(nodeTask.Id, DownloadEndEvent, statusMessage,
                       nodeTask.WorkflowId);
        eventMessage.EventData = new()
        {
            { "numberOfFiles", numberOfFiles.ToString()},
            { "totalSizeInBytes", totalSizeInBytes.ToString()},
            { "errorMessage", errorMessage??string.Empty}
        };
        await PublishAsync(eventMessage);
    }

    public virtual async Task PublishTaskCommencementEventAsync(NodeTask nodeTask)
    {
        var eventMessage = CreateNewEventMessage(nodeTask.Id, TaskCommencementEvent, StartedStatus,
                       nodeTask.WorkflowId);

        await PublishAsync(eventMessage);
    }

    public async Task PublishTaskCompletionEventAsync(NodeTask tesNodeTask, TimeSpan duration, string statusMessage, string? errorMessage)
    {
        var eventMessage = CreateNewEventMessage(tesNodeTask.Id, TaskCompletionEvent, statusMessage,
            tesNodeTask.WorkflowId);
        eventMessage.EventData = new()
        {
            { "duration", duration.ToString()},
            { "errorMessage", errorMessage??string.Empty}
        };

        await PublishAsync(eventMessage);
    }

    private static EventMessage CreateNewEventMessage(string? entityId, string name, string statusMessage,
        string? correlationId)
    {
        return new EventMessage
        {
            Id = Guid.NewGuid().ToString(),
            Name = name,
            StatusMessage = statusMessage,
            EntityType = TesTaskRunnerEntityType,
            CorrelationId = correlationId ?? Guid.NewGuid().ToString(),
            EntityId = entityId ?? Guid.NewGuid().ToString(),
            Created = DateTime.UtcNow,
            EventVersion = EventVersion,
            EventDataVersion = EventDataVersion,
            EventData = []
        };
    }


    private async Task PublishAsync(EventMessage message)
    {
        if (sinks.Count == 0)
        {
            logger.LogWarning("No sinks configured for publishing events");
            return;
        }

        foreach (var sink in sinks)
        {
            logger.LogDebug("Publishing event {MessageName} to sink: {SinkType}", message.Name, sink.GetType().Name);

            await sink.PublishEventAsync(message);
        }
    }

    public async Task FlushPublishersAsync(int waitTimeInSeconds = 60)
    {
        var stopTasks = sinks.Select(s => s.StopAsync());

        await Task.WhenAll(stopTasks).WaitAsync(TimeSpan.FromSeconds(waitTimeInSeconds));
    }

    public async ValueTask DisposeAsync()
    {
        await FlushPublishersAsync();
    }
}
