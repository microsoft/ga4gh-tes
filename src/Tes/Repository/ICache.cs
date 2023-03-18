// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Repository
{
    public interface ICache<T>
    {
        int Count();
        int MaxSize { get; set; }
        bool TryGetValue(string key, out T task);
        bool TryAdd(string key, T task);
        bool TryUpdate(string key, T task);
        bool TryRemove(string key);
    }
}
