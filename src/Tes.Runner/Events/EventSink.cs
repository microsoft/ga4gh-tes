// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Tes.Runner.Transfer;

namespace Tes.Runner.Events
{
    public abstract class EventSink : IEventSink
    {
        const int StopWaitDurationInSeconds = 60;

        private readonly Channel<EventMessage> events = Channel.CreateUnbounded<EventMessage>();
        private readonly ILogger logger = PipelineLoggerFactory.Create<EventSink>();
        private Task? eventHandlerTask;

        public abstract Task HandleEventAsync(EventMessage eventMessage);

        public async Task PublishEventAsync(EventMessage eventMessage)
        {
            await events.Writer.WriteAsync(eventMessage);
        }

        public void Start()
        {
            logger.LogDebug("Starting events processing handler");

            eventHandlerTask = Task.Run(async () => await EventHandlerAsync());
        }


        public async Task StopAsync()
        {
            if (eventHandlerTask is null)
            {
                throw new InvalidOperationException("Event handler task is not running");
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
                { "created", eventMessage.Created.ToString("yyyy-MM-ddTHH:mm:ssZ") }
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
                        logger.LogDebug($"Handling event. Event Name: {eventMessage.Name} Event ID: {eventMessage.Id} ");

                        await HandleEventAsync(eventMessage);

                        logger.LogDebug($"Event handled. Event Name: {eventMessage.Name} Event ID: {eventMessage.Id} ");
                    }
                    catch (Exception e)
                    {
                        //event error should be handled silently, as event failures should not affect overall processing....
                        logger.LogError(e, $"Error handling event. Event ID: {eventMessage.Id}");
                    }
                }
            }
        }
    }
}
