// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Tes.Runner.Transfer;

namespace Tes.Runner.Events
{
    public abstract class EventSink : IEventSink
    {
        const double StopDelay = 30;
        private readonly Channel<EventMessage> events;
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ILogger logger = PipelineLoggerFactory.Create<EventSink>();

        private Task? eventHandlerTask;

        protected EventSink()
        {
            events = Channel.CreateUnbounded<EventMessage>();
        }

        public abstract Task HandleEventAsync(EventMessage eventMessage);
        public Task PublishEventAsync(EventMessage eventMessage)
        {
            throw new NotImplementedException();
        }

        public Task StartAsync()
        {
            logger.LogDebug("Starting events processing handler");

            eventHandlerTask = EventHandlerAsync(cancellationTokenSource.Token);

            return eventHandlerTask;
        }

        public async Task StopAsync()
        {
            logger.LogDebug("Stopping events processing handler");

            if (eventHandlerTask is null)
            {
                throw new InvalidOperationException("Event handler task is not running");
            }

            await Task.WhenAny(eventHandlerTask, Task.Delay(TimeSpan.FromSeconds(StopDelay)));

            logger.LogDebug("Events processing handler is stopped");
        }

        protected IDictionary<string, string> ToEventTag(EventMessage eventMessage)
        {
            return new Dictionary<string, string>
            {
                { "event_name", eventMessage.Name },
                { "event_id", eventMessage.Id },
                { "entity_type", eventMessage.EntityType },
                { "entity_id", eventMessage.EntityId },
                { "correlation_id", eventMessage.CorrelationId },
                //format date to ISO 8601, which is URL friendly
                { "created", eventMessage.Created.ToString("yyyy-MM-ddTHH:mm:ssZ") }
            };
        }
        private async Task EventHandlerAsync(CancellationToken cancellationToken)
        {
            while (await events.Reader.WaitToReadAsync(cancellationToken))
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
                        //event error should be handled silently....
                        logger.LogError(e, $"Error handling event. Event ID: {eventMessage.Id}");
                    }
                }
            }
        }
    }
}
