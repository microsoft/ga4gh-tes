// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Tes.Runner.Events
{
    public abstract class EventSink(ILogger logger) : IEventSink
    {
        public const string Iso8601DateFormat = "yyyy-MM-ddTHH:mm:ss.fffZ";

        const int StopWaitDurationInSeconds = 30;

        private readonly Channel<EventMessage> events = Channel.CreateUnbounded<EventMessage>();
        protected readonly ILogger Logger = logger ?? throw new ArgumentNullException(nameof(logger));
        private Task? eventHandlerTask;

        public abstract Task HandleEventAsync(EventMessage eventMessage);

        public async Task PublishEventAsync(EventMessage eventMessage)
        {
            await events.Writer.WriteAsync(eventMessage);
        }

        public void Start()
        {
            Logger.LogDebug("Starting events processing handler");

            eventHandlerTask = Task.Run(async () => await EventHandlerAsync());
        }


        public async Task StopAsync()
        {
            if (eventHandlerTask is null)
            {
                throw new InvalidOperationException("Event handler task is not running");
            }

            if (eventHandlerTask.IsCompleted)
            {
                Logger.LogDebug("Task already completed");
                return;
            }

            //close the channel to stop the event handler
            events.Writer.Complete();

            await eventHandlerTask.WaitAsync(TimeSpan.FromSeconds(StopWaitDurationInSeconds));
        }

        protected IDictionary<string, string> ToEventTag(EventMessage eventMessage)
        {
            return new Dictionary<string, string>
            {
                { "event_name", eventMessage.Name },
                { "event_id", eventMessage.Id },
                { "entity_type", eventMessage.EntityType },
                { "task_id", eventMessage.EntityId },
                { "workflow_id", eventMessage.CorrelationId },
                //format date to ISO 8601, which is URL friendly
                { "created", eventMessage.Created.ToString(Iso8601DateFormat) }
            };
        }
        private async Task EventHandlerAsync()
        {
            while (await events.Reader.WaitToReadAsync())
            {
                while (events.Reader.TryRead(out var eventMessage))
                {
                    try
                    {
                        Logger.LogDebug($"Handling event. Event Name: {eventMessage.Name} Event ID: {eventMessage.Id} ");

                        await HandleEventAsync(eventMessage);

                        Logger.LogDebug($"Event handled. Event Name: {eventMessage.Name} Event ID: {eventMessage.Id} ");
                    }
                    catch (Exception e)
                    {
                        //event error should be handled silently, as event failures should not affect overall processing....
                        Logger.LogError(e, $"Error handling event. Event ID: {eventMessage.Id}");
                    }
                }
            }
        }
    }
}
