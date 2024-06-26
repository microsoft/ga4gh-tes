// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Runner.Events;

namespace Tes.Runner.Test.Events;

public class TestEventSink : EventSink
{
    public List<EventMessage> EventsHandled { get; } = new List<EventMessage>();
    public int Delay { get; set; } = 0;

    public override async Task HandleEventAsync(EventMessage eventMessage)
    {
        EventsHandled.Add(eventMessage);

        await Task.Delay(Delay);
    }
}
