namespace Tes.Runner.Transfer;

public interface IBlobPipeline
{
    ValueTask<int> ExecuteWriteAsync(PipelineBuffer buffer);
    ValueTask<int> ExecuteReadAsync(PipelineBuffer buffer);

    /// <summary>
    /// This method must return the length in bytes of the source provided. The source is either a URL or path to file.
    /// </summary>
    /// <param name="source">Source path or URL</param>
    /// <returns>Size of the source</returns>
    Task<long> GetSourceLengthAsync(string source);

    Task OnCompletionAsync(long length, Uri? blobUrl, string fileName);

    void ConfigurePipelineBuffer(PipelineBuffer buffer);
}
