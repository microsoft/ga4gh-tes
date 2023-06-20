// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Transfer;

/// <summary>
/// Provides system information.
/// </summary>
public interface ISystemInfoProvider
{
    /// <summary>
    /// Gets the total memory in bytes.
    /// </summary>
    long TotalMemory { get; }
    /// <summary>
    /// Get the number of processors.
    /// </summary>
    int ProcessorCount { get; }
}
