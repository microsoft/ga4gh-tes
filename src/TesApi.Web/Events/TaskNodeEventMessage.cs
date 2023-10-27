// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;
using System;

namespace TesApi.Web.Events
{
    /// <summary>
    /// <see cref="Tes.Runner.Events.EventMessage"/> from blob storage for processing by TES server.
    /// </summary>
    /// <param name="BlobUri">URL of the event message.</param>
    /// <param name="Tags">Tags on the event message blob.</param>
    /// <param name="Event">Name of the event based on parsing the blob's BlobName.</param>
    public record class TaskNodeEventMessage(Uri BlobUri, IDictionary<string, string> Tags, string Event)
    {
        /// <summary>
        /// 
        /// </summary>
        public Tes.Runner.Events.EventMessage RunnerEventMessage { get; private set; }

        /// <summary>
        /// 
        /// </summary>
        public string TesTaskId => RunnerEventMessage?.EntityId;

        /// <summary>
        /// Sets <see cref="RunnerEventMessage"/>.
        /// </summary>
        /// <param name="eventMessage">The downloaded event message associated with this storage blob.</param>
        public void SetRunnerEventMessage(Tes.Runner.Events.EventMessage eventMessage)
        {
            ArgumentNullException.ThrowIfNull(eventMessage);

            if (RunnerEventMessage is not null)
            {
                throw new InvalidOperationException("RunnerEventMessage has already been set.");
            }

            RunnerEventMessage = eventMessage;
        }
    }
}
