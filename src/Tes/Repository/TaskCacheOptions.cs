// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Models;

namespace Tes.Repository
{
    public class TaskCacheOptions
    {
        public const string SectionName = "TaskCacheOptions";

        public int TesTaskCacheMaxMemoryBytes { get; set; } = ConcurrentDictionaryCache<TesTask>.DefaultMaxMemoryBytes;
        public int TesTaskCacheMaxObjectSizeBytes { get; set; } = ConcurrentDictionaryCache<TesTask>.DefaultMaxObjectSizeBytes;
    }
}
