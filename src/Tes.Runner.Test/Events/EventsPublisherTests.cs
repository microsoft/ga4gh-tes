// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Runner.Events;
using Tes.Runner.Models;

namespace Tes.Runner.Test.Events
{
    [TestClass, TestCategory("Unit")]
    public class EventsPublisherTests
    {
        private List<IEventSink> sinks = null!;
        private EventsPublisher eventsPublisher = null!;
        private NodeTask nodeTask = null!;

        [TestInitialize]
        public void SetUp()
        {
            nodeTask = new NodeTask()
            {
                Id = "testId",
                WorkflowId = "workflowID",
                Executors =
                [
                    new()
                    {
                        ImageName = "image",
                        ImageTag = "tag",
                        CommandsToExecute = ["echo hello"],
                    }
                ],
                Inputs =
                [
                    new()
                    {
                        Path = "/mnt/data/input1.txt",
                        SourceUrl = "https://test.blob.core.windows.net/test/input1.txt"
                    }
                ],
                Outputs =
                [
                    new()
                    {
                        Path = "/mnt/data/output1.txt",
                        TargetUrl = "https://test.blob.core.windows.net/test/output1.txt"
                    }
                ]
            };

            var sink = new TestEventSink();
            sink.Start();
            sinks = [sink];
            eventsPublisher = new EventsPublisher(sinks);
        }

        [TestMethod]
        public async Task PublishUploadStartEventAsync_EventIsPublished_EventContainsAllExpectedData()
        {
            await eventsPublisher.PublishUploadStartEventAsync(nodeTask);
            await eventsPublisher.FlushPublishersAsync();

            var eventMessage = ((TestEventSink)sinks[0]).EventsHandled[0];

            AssertMessageBaseMapping(eventMessage, EventsPublisher.UploadStartEvent, EventsPublisher.StartedStatus);
        }

        [TestMethod]
        public async Task PublishUploadEndEventAsync_EventIsPublished_EventContainsAllExpectedData()
        {
            await eventsPublisher.PublishUploadEndEventAsync(nodeTask, numberOfFiles: 1, totalSizeInBytes: 100, EventsPublisher.SuccessStatus);
            await eventsPublisher.FlushPublishersAsync();

            var eventMessage = ((TestEventSink)sinks[0]).EventsHandled[0];

            AssertMessageBaseMapping(eventMessage, EventsPublisher.UploadEndEvent, EventsPublisher.SuccessStatus);
            Assert.AreEqual(1, int.Parse(eventMessage.EventData!["numberOfFiles"]));
            Assert.AreEqual(100, int.Parse(eventMessage.EventData!["totalSizeInBytes"]));
            Assert.AreEqual("", eventMessage.EventData!["errorMessage"]);
        }

        [TestMethod]
        public async Task PublishDownloadStartEventAsync_EventIsPublished_EventContainsAllExpectedData()
        {
            await eventsPublisher.PublishDownloadStartEventAsync(nodeTask);
            await eventsPublisher.FlushPublishersAsync();

            var eventMessage = ((TestEventSink)sinks[0]).EventsHandled[0];

            AssertMessageBaseMapping(eventMessage, EventsPublisher.DownloadStartEvent, EventsPublisher.StartedStatus);
        }

        [TestMethod]
        public async Task PublishDownloadEndEventAsync_EventIsPublished_EventContainsAllExpectedData()
        {
            await eventsPublisher.PublishDownloadEndEventAsync(nodeTask, numberOfFiles: 1, totalSizeInBytes: 100, EventsPublisher.SuccessStatus);
            await eventsPublisher.FlushPublishersAsync();

            var eventMessage = ((TestEventSink)sinks[0]).EventsHandled[0];

            AssertMessageBaseMapping(eventMessage, EventsPublisher.DownloadEndEvent, EventsPublisher.SuccessStatus);
            Assert.AreEqual(1, int.Parse(eventMessage.EventData!["numberOfFiles"]));
            Assert.AreEqual(100, int.Parse(eventMessage.EventData!["totalSizeInBytes"]));
            Assert.AreEqual("", eventMessage.EventData!["errorMessage"]);
        }

        [TestMethod]
        public async Task PublishExecutorStartEventAsync_EventIsPublished_EventContainsAllExpectedData()
        {
            await eventsPublisher.PublishExecutorStartEventAsync(nodeTask, 0);
            await eventsPublisher.FlushPublishersAsync();

            var eventMessage = ((TestEventSink)sinks[0]).EventsHandled[0];

            AssertMessageBaseMapping(eventMessage, EventsPublisher.ExecutorStartEvent, EventsPublisher.StartedStatus);
            Assert.AreEqual("1/1", eventMessage.EventData!["executor"]);
            Assert.AreEqual(nodeTask.Executors?[0].ImageName, eventMessage.EventData!["image"]);
            Assert.AreEqual(nodeTask.Executors?[0].ImageTag, eventMessage.EventData!["imageTag"]);
            Assert.AreEqual(nodeTask.Executors?[0].CommandsToExecute?.First(), eventMessage.EventData!["commands"]);
        }

        [TestMethod]
        public async Task PublishExecutorEndEventAsync_EventIsPublished_EventContainsAllExpectedData()
        {
            await eventsPublisher.PublishExecutorEndEventAsync(nodeTask, 0, exitCode: 0, statusMessage: EventsPublisher.SuccessStatus, errorMessage: string.Empty);
            await eventsPublisher.FlushPublishersAsync();

            var eventMessage = ((TestEventSink)sinks[0]).EventsHandled[0];

            AssertMessageBaseMapping(eventMessage, EventsPublisher.ExecutorEndEvent, EventsPublisher.SuccessStatus);
            Assert.AreEqual("1/1", eventMessage.EventData!["executor"]);
            Assert.AreEqual(nodeTask.Executors?[0].ImageName, eventMessage.EventData!["image"]);
            Assert.AreEqual(nodeTask.Executors?[0].ImageTag, eventMessage.EventData!["imageTag"]);
            Assert.AreEqual(0, int.Parse(eventMessage.EventData!["exitCode"]));
            Assert.AreEqual("", eventMessage.EventData!["errorMessage"]);
        }

        [TestMethod]
        public async Task PublishTaskCommencementEventAsync_EventIsPublished_EventContainsAllExpectedData()
        {
            await eventsPublisher.PublishTaskCommencementEventAsync(nodeTask);
            await eventsPublisher.FlushPublishersAsync();

            var eventMessage = ((TestEventSink)sinks[0]).EventsHandled[0];

            AssertMessageBaseMapping(eventMessage, EventsPublisher.TaskCommencementEvent, EventsPublisher.StartedStatus);
        }

        [TestMethod]
        public async Task PublishTaskCompletionEventAsync_EventIsPublished_EventContainsAllExpectedData()
        {
            var duration = TimeSpan.FromSeconds(10);
            await eventsPublisher.PublishTaskCompletionEventAsync(nodeTask, duration, EventsPublisher.SuccessStatus,
                errorMessage: string.Empty);
            await eventsPublisher.FlushPublishersAsync();

            var eventMessage = ((TestEventSink)sinks[0]).EventsHandled[0];

            AssertMessageBaseMapping(eventMessage, EventsPublisher.TaskCompletionEvent, EventsPublisher.SuccessStatus);
            Assert.AreEqual(duration.ToString(), eventMessage.EventData!["duration"]);
            Assert.AreEqual("", eventMessage.EventData!["errorMessage"]);
        }

        private void AssertMessageBaseMapping(EventMessage eventMessage, string eventName, string statusMessage)
        {
            Assert.AreEqual(nodeTask.Id, eventMessage.EntityId);
            Assert.AreEqual(nodeTask.WorkflowId, eventMessage.CorrelationId);
            Assert.AreEqual(eventName, eventMessage.Name);
            Assert.AreEqual(statusMessage, eventMessage.StatusMessage);
        }
    }
}
