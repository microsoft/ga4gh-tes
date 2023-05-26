// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Runner.Models;
using Tes.Runner.Transfer;

namespace Tes.Runner.Storage;
/// <summary>
/// Applies the SAS resolution strategy for task inputs and outputs.
/// </summary>
public class ResolutionPolicyHandler
{
    /// <summary>
    /// Applies SAS resolution strategy to task outputs.
    /// </summary>
    /// <param name="testTaskOutputs"><see cref="FileOutput"/>></param>
    /// <returns>List of <see cref="UploadInfo"/>></returns>
    public async Task<List<UploadInfo>?> ApplyResolutionPolicyAsync(List<FileOutput>? testTaskOutputs)
    {
        if (testTaskOutputs is null)
        {
            return null;
        }

        var list = new List<UploadInfo>();

        foreach (var output in testTaskOutputs)
        {
            list.Add(await CreateUploadInfoWithStrategyAsync(output));
        }

        return list;
    }

    /// <summary>
    /// Applies SAS resolution strategy to task inputs.
    /// </summary>
    /// <param name="tesTaskInputs"><see cref="FileInput"/>></param>
    /// <returns>List of <see cref="DownloadInfo"/></returns>
    public async Task<List<DownloadInfo>?> ApplyResolutionPolicyAsync(List<FileInput>? tesTaskInputs)
    {
        if (tesTaskInputs is null)
        {
            return null;
        }

        var list = new List<DownloadInfo>();

        foreach (var input in tesTaskInputs)
        {
            list.Add(await CreateDownloadInfoWithStrategyAsync(input));
        }

        return list;
    }

    private async Task<DownloadInfo> CreateDownloadInfoWithStrategyAsync(FileInput input)
    {
        if (string.IsNullOrEmpty(input.FullFileName))
        {
            throw new ArgumentException("A task input is missing the full filename. Please check the task definition.");
        }

        var uri = await ApplySasResolutionToUrlAsync(input.SourceUrl, input.SasStrategy);

        return new DownloadInfo(ExpandEnvironmentVariables(input.FullFileName), uri);
    }

    private async Task<UploadInfo> CreateUploadInfoWithStrategyAsync(FileOutput output)
    {
        if (string.IsNullOrEmpty(output.FullFileName))
        {
            throw new ArgumentException("A task output is missing the full filename. Please check the task definition.");
        }

        var uri = await ApplySasResolutionToUrlAsync(output.TargetUrl, output.SasStrategy);

        return new UploadInfo(ExpandEnvironmentVariables(output.FullFileName), uri);
    }

    private static string ExpandEnvironmentVariables(string fullFileName)
    {
        return Environment.ExpandEnvironmentVariables(fullFileName);
    }

    private async Task<Uri> ApplySasResolutionToUrlAsync(string? sourceUrl, SasResolutionStrategy strategy)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceUrl);

        var strategyImpl =
            SasResolutionStrategyFactory.CreateSasResolutionStrategy(strategy);

        return await strategyImpl.CreateSasTokenWithStrategyAsync(sourceUrl);
    }
}
