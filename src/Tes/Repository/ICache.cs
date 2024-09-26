// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;

namespace Tes.Repository
{
    public interface ICache<T>
    {
        int Count();
        int MaxCount { get; set; }
        bool TryGetValue(string key, out T task);
        bool TryAdd(string key, T task);
        bool TryUpdate(string key, T task, TimeSpan expiration = default);
        bool TryRemove(string key);
    }
}
