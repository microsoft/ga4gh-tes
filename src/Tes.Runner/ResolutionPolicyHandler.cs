// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Sas;
using Tes.Runner.Events;
using Tes.Runner.Models;
using Tes.Runner.Storage;
using Tes.Runner.Transfer;

namespace Tes.Runner;

/// <summary>
/// Applies the SAS resolution strategy for task inputs, outputs and event publishers.
/// </summary>
public class ResolutionPolicyHandler
{
    const BlobSasPermissions DownloadBlobSasPermissions = BlobSasPermissions.Read | BlobSasPermissions.List;
    const BlobSasPermissions UploadBlobSasPermissions = BlobSasPermissions.Read | BlobSasPermissions.Write | BlobSasPermissions.Create | BlobSasPermissions.List;

    private readonly UrlTransformationStrategyFactory transformationStrategyFactory = null!;
    private readonly Func<IList<IEventSink>, EventsPublisher> eventsPublisherFactory = null!;

    public ResolutionPolicyHandler(UrlTransformationStrategyFactory transformationStrategyFactory, Func<IList<IEventSink>, EventsPublisher> eventsPublisherFactory)
    {
        ArgumentNullException.ThrowIfNull(transformationStrategyFactory);
        ArgumentNullException.ThrowIfNull(eventsPublisherFactory);

        this.transformationStrategyFactory = transformationStrategyFactory;
        this.eventsPublisherFactory = eventsPublisherFactory;
    }

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

    public async Task<EventsPublisher> CreateEventsPublisherAsync(NodeTask nodeTask, Func<Uri, BlobStorageEventSink> eventSinkFactory)
    {
        var storageSink = await CreateAndStartStorageEventSinkFromTaskIfRequestedAsync(nodeTask, eventSinkFactory);

        List<IEventSink> sinkList = [];
        if (storageSink != null)
        {
            sinkList.Add(storageSink);
        }

        return eventsPublisherFactory(sinkList);
    }

    private async Task<IEventSink?> CreateAndStartStorageEventSinkFromTaskIfRequestedAsync(NodeTask nodeTask, Func<Uri, BlobStorageEventSink> eventSinkFactory)
    {
        ArgumentNullException.ThrowIfNull(nodeTask);

        if (nodeTask.RuntimeOptions.StorageEventSink is null)
        {
            return default;
        }

        if (string.IsNullOrWhiteSpace(nodeTask.RuntimeOptions.StorageEventSink.TargetUrl))
        {
            return default;
        }

        var transformationStrategy = transformationStrategyFactory.CreateStrategy(nodeTask.RuntimeOptions.StorageEventSink.TransformationStrategy);


        var transformedUrl = await transformationStrategy.TransformUrlWithStrategyAsync(
            nodeTask.RuntimeOptions.StorageEventSink.TargetUrl,
             BlobSasPermissions.Write | BlobSasPermissions.Create | BlobSasPermissions.Tag);

        var sink = eventSinkFactory(transformedUrl);

        sink.Start();

        return sink;
    }

    private async Task<DownloadInfo> CreateDownloadInfoWithStrategyAsync(FileInput input,
        BlobSasPermissions downloadBlobSasPermissions)
    {
        if (string.IsNullOrEmpty(input.Path))
        {
            throw new ArgumentException("A task input is missing the path property. Please check the task definition.");
        }

        var uri = await ApplySasResolutionToUrlAsync(input.SourceUrl, input.TransformationStrategy, downloadBlobSasPermissions);

        return new DownloadInfo(input.Path, uri);
    }

    private async Task<UploadInfo> CreateUploadInfoWithStrategyAsync(FileOutput output,
        BlobSasPermissions uploadBlobSasPermissions)
    {
        var uri = await ApplySasResolutionToUrlAsync(output.TargetUrl, output.TransformationStrategy, uploadBlobSasPermissions);

        return new UploadInfo(output.Path!, uri);
    }

    protected virtual async Task<Uri> ApplySasResolutionToUrlAsync(string? sourceUrl, TransformationStrategy? strategy,
        BlobSasPermissions blobSasPermissions)
    {
        ArgumentNullException.ThrowIfNull(strategy);
        ArgumentException.ThrowIfNullOrEmpty(sourceUrl);

        var strategyImpl =
            transformationStrategyFactory.CreateStrategy(strategy.Value);

        return await strategyImpl.TransformUrlWithStrategyAsync(sourceUrl, blobSasPermissions);
    }
}
