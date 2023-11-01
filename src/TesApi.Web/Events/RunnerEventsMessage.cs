// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;

namespace TesApi.Web.Events
{
    /// <summary>
    /// <see cref="Tes.Runner.Events.EventMessage"/> from blob storage for processing by TES server.
    /// </summary>
    /// <param name="BlobUri">URL of the event message.</param>
    /// <param name="Tags">Tags on the event message blob.</param>
    /// <param name="Event">Name of the event based on parsing the blob's BlobName.</param>
    /// <param name="RunnerEventMessage">The content of the event message.</param>
    public record struct RunnerEventsMessage(Uri BlobUri, IDictionary<string, string> Tags, string Event, Tes.Runner.Events.EventMessage RunnerEventMessage = default)
    {
        /// <summary>
        /// Copy constructor replacing <see cref="RunnerEventMessage"/>.
        /// </summary>
        /// <param name="original"></param>
        /// <param name="runnerEventMessage"></param>
        public RunnerEventsMessage(RunnerEventsMessage original, Tes.Runner.Events.EventMessage runnerEventMessage)
            : this(original.BlobUri, original.Tags, original.Event, runnerEventMessage)
        {
        }

        /// <summary>
        /// 
        /// </summary>
        public readonly string TesTaskId => RunnerEventMessage?.EntityId;
    }
}
