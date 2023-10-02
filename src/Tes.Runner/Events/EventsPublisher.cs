// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Logging;
using Tes.Runner.Models;
using Tes.Runner.Storage;
using Tes.Runner.Transfer;
using static Azure.Storage.Sas.BlobSasPermissions;

namespace Tes.Runner.Events;

public class EventsPublisher
{
    const string EventVersion = "1.0";
    const string EventDataVersion = "1.0";
    const string TesTaskRunnerEntityType = "TesRunnerTask";
    const string DownloadStartEvent = "DownloadStart";
    const string DownloadEndEvent = "DownloadEnd";
    const string UploadStartEvent = "UploadStart";
    const string UploadEndEvent = "UploadEnd";
    const string ExecutorStartEvent = "ExecutorStar";
    const string ExecutorEndEvent = "ExecutorEnd";

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
        this.sinks = new List<IEventSink>();
    }

    public static async Task<EventsPublisher> CreateEventsPublisherAsync(NodeTask nodeTask)
    {
        var storageSink = await GetStorageEventSinkFromTaskIfRequestedAsync(nodeTask);

        var sinkList = new List<IEventSink>();
        if (storageSink != null)
        {
            sinkList.Add(storageSink);
        }

        return new EventsPublisher(sinkList);
    }

    private static async Task<IEventSink?> GetStorageEventSinkFromTaskIfRequestedAsync(NodeTask nodeTask)
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
            nodeTask.RuntimeOptions);


        var transformedUrl = await transformationStrategy.TransformUrlWithStrategyAsync(
            nodeTask.RuntimeOptions.StorageEventSink.TargetUrl,
            Read | Create | Write | Add | List);

        return new BlobStorageEventSink(transformedUrl);

    }

    public virtual async Task PublishUploadEventStartAsync(NodeTask nodeTask)
    {
        var eventMessage = CreateNewEventMessage(nodeTask.Id, UploadStartEvent, StartedStatus,
            nodeTask.WorkflowId);

        eventMessage.EventData = new Dictionary<string, string>();

        await PublishAsync(eventMessage);
    }

    public virtual async Task PublishUploadEventEndAsync(NodeTask nodeTask, int numberOfFiles, long totalSizeInBytes, string statusMessage, string? errorMessage = default)
    {
        var eventMessage = CreateNewEventMessage(nodeTask.Id, UploadEndEvent, statusMessage,
            nodeTask.WorkflowId);

        eventMessage.EventData = new Dictionary<string, string>
        {
            { "number_of_files", numberOfFiles.ToString()},
            { "total_size_in_bytes", totalSizeInBytes.ToString()},
            { "error_message", errorMessage??string.Empty}
        };

        await PublishAsync(eventMessage);
    }

    public virtual async Task PublishExecutorEventStartAsync(NodeTask nodeTask)
    {
        var eventMessage = CreateNewEventMessage(nodeTask.Id, ExecutorStartEvent, StartedStatus,
                       nodeTask.WorkflowId);

        var commands = nodeTask.CommandsToExecute ?? new List<string>();

        eventMessage.EventData = new Dictionary<string, string>
        {
            { "image", nodeTask.ImageName!},
            { "image_tag", nodeTask.ImageTag!},
            { "commands", string.Join(' ', commands) }
        };
        await PublishAsync(eventMessage);
    }

    public virtual async Task PublishExecutorEventEndAsync(NodeTask nodeTask, long exitCode, string statusMessage, string? errorMessage = default)
    {
        var eventMessage = CreateNewEventMessage(nodeTask.Id, ExecutorEndEvent, statusMessage,
                                  nodeTask.WorkflowId);
        eventMessage.EventData = new Dictionary<string, string>
        {
            { "image", nodeTask.ImageName!},
            { "image_tag", nodeTask.ImageTag!},
            { "exit_code", exitCode.ToString()},
            { "error_message", errorMessage??string.Empty}
        };
        await PublishAsync(eventMessage);
    }

    public virtual async Task PublishDownloadEventStartAsync(NodeTask nodeTask)
    {
        var eventMessage = CreateNewEventMessage(nodeTask.Id, DownloadStartEvent, StartedStatus,
                       nodeTask.WorkflowId);

        eventMessage.EventData = new Dictionary<string, string>();

        await PublishAsync(eventMessage);
    }

    public virtual async Task PublishDownloadEventEndAsync(NodeTask nodeTask, int numberOfFiles, long totalSizeInBytes, string statusMessage, string? errorMessage = default)
    {
        var eventMessage = CreateNewEventMessage(nodeTask.Id, DownloadEndEvent, statusMessage,
                       nodeTask.WorkflowId);
        eventMessage.EventData = new Dictionary<string, string>
        {
            { "number_of_files", numberOfFiles.ToString()},
            { "total_size_in_bytes", totalSizeInBytes.ToString()},
            { "error_message", errorMessage??string.Empty}
        };
        await PublishAsync(eventMessage);
    }

    private EventMessage CreateNewEventMessage(string? entityId, string name, string statusMessage,
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
            EventDataVersion = EventDataVersion
        };
    }


    private async Task PublishAsync(EventMessage message)
    {
        if (sinks.Count == 0)
        {
            logger.LogTrace("No sinks configured for publishing events");
            return;
        }

        foreach (var sink in sinks)
        {
            logger.LogTrace($"Publishing event {message.Name} to sink: {sink.GetType().Name}");

            await sink.PublishEventAsync(message);
        }
    }
}
