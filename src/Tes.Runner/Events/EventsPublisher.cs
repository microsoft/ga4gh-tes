// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Logging;
using Tes.Runner.Models;
using Tes.Runner.Transfer;

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
    private static EventsPublisher instance = null!;
    private static ILogger logger = PipelineLoggerFactory.Create<EventsPublisher>();

    public const string SuccessStatus = "Success";
    public const string FailedStatus = "Failed";
    public const string StartedStatus = "Started";

    public static EventsPublisher Instance
    {
        get
        {
            if (instance == null)
            {
                throw new InvalidOperationException("EventsPublisher is not initialized");
            }
            return instance;
        }
    }

    public static void Initialize(IList<IEventSink> sinks)
    {
        logger.LogTrace("Initializing EventsPublisher");

        if (instance != null)
        {
            throw new InvalidOperationException("EventsPublisher is already initialized");
        }
        instance = new EventsPublisher(sinks);
    }


    public EventsPublisher(IList<IEventSink> sinks)
    {
        this.sinks = sinks;
    }

    public async Task PublishUploadEventStart(NodeTask nodeTask, int numberOfFiles)
    {
        var eventMessage = CreateNewEventMessage(nodeTask.Id, UploadStartEvent, StartedStatus,
            nodeTask.WorkflowId);

        eventMessage.EventData = new Dictionary<string, string>
        {
            { "number_of_files", numberOfFiles.ToString()}
        };

        await PublishAsync(eventMessage);
    }

    public async Task PublishUploadEventEnd(NodeTask nodeTask, int numberOfFiles, long totalSizeInBytes, string statusMessage, string? errorMessage = default)
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

    public async Task PublishExecutorEventStart(NodeTask nodeTask, string cpuCores, string memSize)
    {
        var eventMessage = CreateNewEventMessage(nodeTask.Id, ExecutorStartEvent, StartedStatus,
                       nodeTask.WorkflowId);

        var commands = nodeTask.CommandsToExecute ?? new List<string>();

        eventMessage.EventData = new Dictionary<string, string>
        {
            { "image", nodeTask.ImageName!},
            { "image_tag", nodeTask.ImageTag!},
            { "cpu_cores", cpuCores},
            { "mem_size", memSize},
            { "commands", string.Join(' ', commands) }
        };
        await PublishAsync(eventMessage);
    }

    public async Task PublishExecutorEventEnd(NodeTask nodeTask, int exitCode, string statusMessage, string? errorMessage = default)
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

    public async Task PublishDownloadEventStart(NodeTask nodeTask, int numberOfFiles)
    {
        var eventMessage = CreateNewEventMessage(nodeTask.Id, DownloadStartEvent, StartedStatus,
                       nodeTask.WorkflowId);
        eventMessage.EventData = new Dictionary<string, string>
        {
            { "number_of_files", numberOfFiles.ToString()}
        };
        await PublishAsync(eventMessage);
    }

    public async Task PublishDownloadEventEnd(NodeTask nodeTask, int numberOfFiles, long totalSizeInBytes, string statusMessage, string? errorMessage = default)
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
        foreach (var sink in sinks)
        {
            logger.LogTrace($"Publishing event {message.Name} to sink: {sink.GetType().Name}");

            await sink.PublishEventAsync(message);
        }
    }
}
