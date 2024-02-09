// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Events
{
    public interface IEventSink
    {
        Task PublishEventAsync(EventMessage eventMessage);
        void Start();
        Task StopAsync();
    }
}
