﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Tes.Runner.Transfer;

namespace Tes.Runner.Events
{
    public abstract class EventSink : IEventSink
    {
        public const string Iso8601DateFormat = "yyyy-MM-ddTHH:mm:ss.fffZ";

        const int StopWaitDurationInSeconds = 30;

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
            logger.LogTrace("Starting events processing handler");

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
                logger.LogTrace("Task already completed");
                return;
            }

            //close the channel to stop the event handler
            events.Writer.Complete();

            await eventHandlerTask.WaitAsync(TimeSpan.FromSeconds(StopWaitDurationInSeconds));
        }

        protected static IDictionary<string, string> ToEventTag(EventMessage eventMessage)
        {
            return new Dictionary<string, string>
            {
                { "task-id", eventMessage.EntityId },
                { "workflow-id", eventMessage.CorrelationId },
                { "event-name", eventMessage.Name },
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
                        logger.LogTrace($"Handling event. Event Name: {eventMessage.Name} Event ID: {eventMessage.Id} ");

                        await HandleEventAsync(eventMessage);

                        logger.LogTrace($"Event handled. Event Name: {eventMessage.Name} Event ID: {eventMessage.Id} ");
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
