// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Channels;
using Tes.Runner.Events;

namespace Tes.Runner.Test.Events
{
    [TestClass, TestCategory("Unit")]
    public class EventSinkTests
    {
        private TestEventSink eventSink = null!;

        [TestInitialize]
        public void SetUp()
        {
            eventSink = new TestEventSink();
        }

        [TestMethod]
        public async Task Start_SinkIsStartedEventPublishedStopped_HandleReceivesEvent()
        {
            eventSink.Start();

            await eventSink.PublishEventAsync(new EventMessage());

            await eventSink.StopAsync();

            Assert.AreEqual(1, eventSink.EventsHandled.Count);
        }

        [TestMethod]
        public async Task Start_SinkIsStartedEventsPublishedStopped_HandleReceivesEvents()
        {
            eventSink.Start();

            await eventSink.PublishEventAsync(new EventMessage());
            await eventSink.PublishEventAsync(new EventMessage());
            await eventSink.PublishEventAsync(new EventMessage());

            await eventSink.StopAsync();

            Assert.AreEqual(3, eventSink.EventsHandled.Count);
        }

        [TestMethod]
        public async Task Start_SinkIsStartedEventsPublishedDelayedProcessingStopped_HandleReceivesEvents()
        {
            eventSink.Delay = 10;

            eventSink.Start();

            var tasks = new List<Task>();

            tasks.Add(eventSink.PublishEventAsync(new EventMessage()));
            tasks.Add(eventSink.PublishEventAsync(new EventMessage()));
            tasks.Add(eventSink.PublishEventAsync(new EventMessage()));

            await eventSink.StopAsync();

            Assert.AreEqual(3, eventSink.EventsHandled.Count);

            foreach (var task in tasks)
            {
                Assert.IsTrue(task.IsCompleted);
            }
        }

        [TestMethod]
        public async Task Stop_MultipleEventsAreSetnConcurrently_StopWaitsForAll()
        {
            eventSink.Start();

            var tasks = new List<Task>();

            tasks.Add(eventSink.PublishEventAsync(new EventMessage()));
            tasks.Add(eventSink.PublishEventAsync(new EventMessage()));
            tasks.Add(eventSink.PublishEventAsync(new EventMessage()));

            await eventSink.StopAsync();

            Assert.AreEqual(3, eventSink.EventsHandled.Count);

            foreach (var task in tasks)
            {
                Assert.IsTrue(task.IsCompleted);
            }
        }

        [TestMethod]
        public async Task Stop_SinkIsStoppedEventPublished_Throws()
        {
            eventSink.Start();
            await eventSink.PublishEventAsync(new EventMessage());
            await eventSink.StopAsync();
            await Assert.ThrowsExceptionAsync<ChannelClosedException>(() => eventSink.PublishEventAsync(new EventMessage()));
        }
    }
}
