// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Concurrent;
using Tes.Models;

namespace TesApi.Web
{
    public interface ITaskQueueItemProcessor
    {
        TaskQueueItem UpdateTaskQueueItem(string key, TaskQueueItem item);
    }


    public class TaskQueueItemProcessor : ITaskQueueItemProcessor
    {
        public ConcurrentDictionary<string, TaskQueueItem> taskQueueItems = new ConcurrentDictionary<string, TaskQueueItem>();

        public TaskQueueItem UpdateTaskQueueItem(string key, TaskQueueItem item)
        {
            return taskQueueItems.AddOrUpdate(key, item, (existingKey, existingValue) => item);
        }
    }
}
