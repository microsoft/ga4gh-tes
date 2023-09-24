// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;

namespace Tes.Models
{
    public class TaskQueueItemBasic
    {
        public string Id { get; set; }
        public DateTime? LastPing { get; set; }
        public TaskQueueItemState State { get; set; }
    }

    public class TaskQueueItemFull : TaskQueueItemBasic
    {
        public TesTask TesTask { get; set; }

        public static TaskQueueItemFull FromBasic(TaskQueueItemBasic basic, TesTask tesTask)
        {
            return new TaskQueueItemFull
            {
                Id = basic.Id,
                LastPing = basic.LastPing,
                State = basic.State,
                TesTask = tesTask
            };
        }
        public static TaskQueueItemFull CreateNew(TesTask tesTask)
        {
            return new TaskQueueItemFull
            {
                // Id has no hyphens for TES Task ID consistency
                // Use the existing ID if it exists, otherwise generate a new one (should only happen during upgrade)
                Id = Guid.NewGuid().ToString("N"),
                TesTask = tesTask
            };
        }

        public static TaskQueueItemFull CreateFromExisting(TesTask tesTask)
        {
            return new TaskQueueItemFull
            {
                // Id has no hyphens for TES Task ID consistency
                // Use the existing ID if it exists, otherwise generate a new one (should only happen during upgrade)
                Id = tesTask.TaskQueueItemId,
                TesTask = tesTask
            };
        }
    }

    public enum TaskQueueItemState
    {
        Queued = 0,
        Running,
        Complete,
        Failed
    }
}
