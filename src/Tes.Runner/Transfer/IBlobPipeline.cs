// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Transfer;

/// <summary>
/// Represents a pipeline that can be used to transfer data from and to blob storage.
/// </summary>
public interface IBlobPipeline
{
    /// <summary>
    /// Writes the buffer's data to the destination.
    /// </summary>
    /// <param name="buffer"><see cref="PipelineBuffer"/></param>
    /// <returns>Number of bytes written</returns>
    ValueTask<int> ExecuteWriteAsync(PipelineBuffer buffer);

    /// <summary>
    /// Reads from the source and writes to the buffer.
    /// </summary>
    /// <param name="buffer"><see cref="PipelineBuffer"/></param>
    /// <returns>Number of bytes read into the buffer</returns>
    ValueTask<int> ExecuteReadAsync(PipelineBuffer buffer);

    /// <summary>
    /// Returns the length in bytes of the source provided. The source is either a URL or path to file.
    /// </summary>
    /// <param name="source">Source path or URL</param>
    /// <returns>Size of the source</returns>
    Task<long> GetSourceLengthAsync(string source);

    /// <summary>
    /// Called when the pipeline has completed the processing of a file or blob.
    /// </summary>
    /// <param name="length">Blob length in bytes</param>
    /// <param name="blobUrl">Url to the blob in azure storage</param>
    /// <param name="fileName">Path to the file</param>
    /// <param name="rootHash"></param>
    /// <returns></returns>
    Task OnCompletionAsync(long length, Uri? blobUrl, string fileName, string rootHash);

    /// <summary>
    /// Called when a buffer is created. This is used to configure the buffer with additional information.
    /// </summary>
    /// <param name="buffer"><see cref="PipelineBuffer"/></param>
    void ConfigurePipelineBuffer(PipelineBuffer buffer);
}
