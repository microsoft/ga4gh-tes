// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Runner.Models;

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
        var eventMessage = CreateNewEventMessage(nodeTask.Id, UploadStartEvent, $"Start uploading {numberOfFiles} files",
            nodeTask.WorkflowId);

        eventMessage.EventData = new Dictionary<string, string>
        {
            { "number_of_files", numberOfFiles.ToString()}
        };

        await PublishAsync(eventMessage);
    }

    public async Task PublishUploadEventEnd(NodeTask nodeTask, int numberOfFiles, long totalSizeInBytes)
    {
        var eventMessage = CreateNewEventMessage(nodeTask.Id, UploadEndEvent, $"Finish uploading {numberOfFiles} files",
            nodeTask.WorkflowId);

        eventMessage.EventData = new Dictionary<string, string>
        {
            { "number_of_files", numberOfFiles.ToString()},
            { "total_size_in_bytes", totalSizeInBytes.ToString()}
        };

        await PublishAsync(eventMessage);
    }

    public async Task PublishExecutorEventStart(NodeTask nodeTask, string cpuCores, string memSize)
    {
        var eventMessage = CreateNewEventMessage(nodeTask.Id, ExecutorStartEvent, $"Start executing task",
                       nodeTask.WorkflowId);

        eventMessage.EventData = new Dictionary<string, string>
        {
            { "image", nodeTask.ImageName!},
            { "image_tag", nodeTask.ImageTag!},
            { "cpu_cores", cpuCores},
            { "mem_size", memSize}
        };
        await PublishAsync(eventMessage);
    }

    public async Task PublishExecutorEventEnd(NodeTask nodeTask, string cpuCores, string memSize, int exitCode, string errorMessage)
    {
        var eventMessage = CreateNewEventMessage(nodeTask.Id, ExecutorEndEvent, $"Finish executing task",
                                  nodeTask.WorkflowId);
        eventMessage.EventData = new Dictionary<string, string>
        {
            { "image", nodeTask.ImageName!},
            { "image_tag", nodeTask.ImageTag!},
            { "cpu_cores", cpuCores},
            { "mem_size", memSize},
            { "exit_code", exitCode.ToString()},
            { "error_message", errorMessage}
        };
        await PublishAsync(eventMessage);
    }

    public async Task PublishDownloadEventStart(NodeTask nodeTask, int numberOfFiles)
    {
        var eventMessage = CreateNewEventMessage(nodeTask.Id, DownloadStartEvent, $"Start downloading {numberOfFiles} files",
                       nodeTask.WorkflowId);
        eventMessage.EventData = new Dictionary<string, string>
        {
            { "number_of_files", numberOfFiles.ToString()}
        };
        await PublishAsync(eventMessage);
    }

    public async Task PublishDownloadEventEnd(NodeTask nodeTask, int numberOfFiles, long totalSizeInBytes)
    {
        var eventMessage = CreateNewEventMessage(nodeTask.Id, DownloadEndEvent, $"Finish downloading {numberOfFiles} files",
                       nodeTask.WorkflowId);
        eventMessage.EventData = new Dictionary<string, string>
        {
            { "number_of_files", numberOfFiles.ToString()},
            { "total_size_in_bytes", totalSizeInBytes.ToString()}
        };
        await PublishAsync(eventMessage);
    }

    private EventMessage CreateNewEventMessage(string? entityId, string name, string message,
        string? correlationId)
    {

        return new EventMessage
        {
            Id = Guid.NewGuid().ToString(),
            Name = name,
            Message = message,
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
            await sink.PublishEventAsync(message);
        }
    }
}
