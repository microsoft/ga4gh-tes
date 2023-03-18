// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Models;

namespace Tes.Repository
{
    public class TesOptions
    {
        public const string SectionName = "TesOptions";

        public int TesTaskCacheMaxMemoryBytes { get; set; } = ConcurrentDictionaryCache<TesTask>.DefaultMaxMemoryBytes;
        public int TesTaskCacheMaxObjectSizeBytes { get; set; } = ConcurrentDictionaryCache<TesTask>.DefaultMaxObjectSizeBytes;
    }
}
