// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Sas;
using Tes.Runner.Models;
using Tes.Runner.Transfer;

namespace Tes.Runner.Storage;
/// <summary>
/// Applies the SAS resolution strategy for task inputs and outputs.
/// </summary>
public class ResolutionPolicyHandler
{
    const BlobSasPermissions DownloadBlobSasPermissions = BlobSasPermissions.Read | BlobSasPermissions.List;
    const BlobSasPermissions UploadBlobSasPermissions = BlobSasPermissions.Read | BlobSasPermissions.Write | BlobSasPermissions.Create | BlobSasPermissions.List;

    private readonly RuntimeOptions runtimeOptions = null!;
    private readonly string mountParentDirectoryPath = null!;
    private readonly string apiVersion;

    public ResolutionPolicyHandler(RuntimeOptions runtimeOptions, string apiVersion)
    {
        ArgumentNullException.ThrowIfNull(runtimeOptions);
        ArgumentException.ThrowIfNullOrEmpty(apiVersion);

        this.runtimeOptions = runtimeOptions;
        this.apiVersion = apiVersion;
        this.mountParentDirectoryPath = Environment.ExpandEnvironmentVariables(runtimeOptions.MountParentDirectoryPath ?? throw new ArgumentException($"{nameof(runtimeOptions.MountParentDirectoryPath)} is missing.", nameof(runtimeOptions)));
    }

    /// <summary>
    /// Parameter-less constructor for mocking
    /// </summary>
    protected ResolutionPolicyHandler() { apiVersion = BlobPipelineOptions.DefaultApiVersion; }

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
            if (string.IsNullOrEmpty(output.Path))
            {
                throw new ArgumentException("A task output is missing the path property. Please check the task definition.");
            }

            list.Add(await CreateUploadInfoWithStrategyAsync(output, UploadBlobSasPermissions));
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
            list.Add(await CreateDownloadInfoWithStrategyAsync(input, DownloadBlobSasPermissions));
        }

        return list;
    }

    private async Task<DownloadInfo> CreateDownloadInfoWithStrategyAsync(FileInput input,
        BlobSasPermissions downloadBlobSasPermissions)
    {
        if (string.IsNullOrEmpty(input.Path))
        {
            throw new ArgumentException("A task input is missing the path property. Please check the task definition.");
        }

        var uri = await ApplySasResolutionToUrlAsync(input.SourceUrl, input.TransformationStrategy, downloadBlobSasPermissions, runtimeOptions, apiVersion);

        return new DownloadInfo(input.Path, uri);
    }

    private async Task<UploadInfo> CreateUploadInfoWithStrategyAsync(FileOutput output,
        BlobSasPermissions uploadBlobSasPermissions)
    {
        var uri = await ApplySasResolutionToUrlAsync(output.TargetUrl, output.TransformationStrategy, uploadBlobSasPermissions, runtimeOptions, apiVersion);

        return new UploadInfo(output.Path!, uri, mountParentDirectoryPath);
    }

    protected virtual async Task<Uri> ApplySasResolutionToUrlAsync(string? sourceUrl, TransformationStrategy? strategy,
        BlobSasPermissions blobSasPermissions, RuntimeOptions runtimeOptions, string apiVersion)
    {
        ArgumentNullException.ThrowIfNull(strategy);
        ArgumentException.ThrowIfNullOrEmpty(sourceUrl);

        var strategyImpl =
            UrlTransformationStrategyFactory.CreateStrategy(strategy.Value, runtimeOptions, apiVersion);

        return await strategyImpl.TransformUrlWithStrategyAsync(sourceUrl, blobSasPermissions);
    }
}
