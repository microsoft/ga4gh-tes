// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using Tes.Runner;
using Tes.Runner.Models;
using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands;

[JsonSourceGenerationOptions(PropertyNameCaseInsensitive = true, DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingDefault)]
[JsonSerializable(typeof(NodeTask))]
[JsonSerializable(typeof(NodeTaskResolverOptions))]
public partial class NodeTaskContext : JsonSerializerContext
{ }

public class NodeTaskResolver(ILogger<NodeTaskResolver> logger)
{
    private readonly ILogger Logger = logger;

    /// <summary>
    /// Parameter-less constructor for mocking
    /// </summary>
    protected NodeTaskResolver() : this(Microsoft.Extensions.Logging.Abstractions.NullLogger<NodeTaskResolver>.Instance) { }

    private static T DeserializeJson<T>(ReadOnlySpan<byte> json, System.Text.Json.Serialization.Metadata.JsonTypeInfo<T> typeInfo)
    {
        return JsonSerializer.Deserialize(json, typeInfo) ?? throw new System.Diagnostics.UnreachableException("Failure to deserialize JSON.");
    }

    public async Task<NodeTask> DeserializeNodeTaskAsync(FileInfo tesNodeTaskFile)
    {
        try
        {
            using MemoryStream memoryStream = new();

            {
                using var stream = tesNodeTaskFile.OpenRead();
                await stream.CopyToAsync(memoryStream);
            }

            var nodeTask = DeserializeJson(memoryStream.ToArray(), NodeTaskContext.Default.NodeTask);

            AddDefaultValuesIfMissing(nodeTask);

            return nodeTask;
        }
        catch (Exception e)
        {
            Logger.LogError(e, "Failed to deserialize task JSON file.");
            throw;
        }
    }

    public async Task SerializeNodeTaskAsync(NodeTask nodeTask, FileInfo tesNodeTaskFile)
    {
        try
        {
            var nodeTaskText = JsonSerializer.Serialize(nodeTask, NodeTaskContext.Default.NodeTask) ?? throw new InvalidOperationException("The JSON data provided is invalid.");

            using var stream = tesNodeTaskFile.OpenWrite();
            await stream.WriteAsync(System.Text.Encoding.UTF8.GetBytes(nodeTaskText));
        }
        catch (Exception e)
        {
            Logger.LogError(e, "Failed to serialize task JSON file.");
            throw;
        }
    }

    public async Task<NodeTask> ResolveNodeTaskAsync(FileInfo? file, Uri? uri, string apiVersion, bool saveDownload = false)
    {
        return await ResolveNodeTaskAsync(file, uri, apiVersion, saveDownload, GetNodeTaskResolverOptions());
    }

    private async Task<NodeTask> ResolveNodeTaskAsync(FileInfo? file, Uri? uri, string apiVersion, bool saveDownload, Lazy<NodeTaskResolverOptions> options)
    {
        file?.Refresh();

        if (file?.Exists ?? false)
        {
            return await DeserializeNodeTaskAsync(file);
        }

        ArgumentNullException.ThrowIfNull(uri);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(apiVersion);

        return await GetNodeTaskDownloader(
            downloader => downloader(Logger).ResolveNodeTaskAsync(file, uri, apiVersion, saveDownload, options),
            ConfigureServicesParameters(
                options.Value.RuntimeOptions ?? throw new InvalidOperationException($"Environment variable '{nameof(NodeTaskResolverOptions)}' is missing the '{nameof(NodeTaskResolverOptions.RuntimeOptions)}' property."),
                apiVersion));
    }

    public virtual Func<Func<Func<ILogger, NodeTaskDownloader>, Task<NodeTask>>, Action<Microsoft.Extensions.Hosting.IHostApplicationBuilder>?, Task<NodeTask>> GetNodeTaskDownloader => Services.BuildAndRunAsync;
    public virtual Func<RuntimeOptions, string, Action<Microsoft.Extensions.Hosting.IHostApplicationBuilder>> ConfigureServicesParameters => Services.ConfigureParameters;

    public class NodeTaskDownloader(NodeTaskResolver parent, ResolutionPolicyHandler resolutionPolicy, Lazy<BlobApiHttpUtils> blobApiHttpUtilsFactory, ILogger logger)
    {
        private readonly NodeTaskResolver parent = parent ?? throw new ArgumentNullException(nameof(parent));
        private readonly ILogger logger = logger ?? throw new ArgumentNullException(nameof(logger));
        private readonly ResolutionPolicyHandler resolutionPolicy = resolutionPolicy ?? throw new ArgumentNullException(nameof(resolutionPolicy));
        private readonly Lazy<BlobApiHttpUtils> blobApiHttpUtils = blobApiHttpUtilsFactory;

        internal async Task<NodeTask> ResolveNodeTaskAsync(FileInfo? file, Uri? uri, string apiVersion, bool saveDownload, Lazy<NodeTaskResolverOptions> options)
        {
            try
            {
                List<FileInput> sources = [new() { TransformationStrategy = options.Value.TransformationStrategy, SourceUrl = uri?.AbsoluteUri, Path = (file ?? new(CommandFactory.DefaultTaskDefinitionFile)).FullName }];
                var blobUri = (await resolutionPolicy.ApplyResolutionPolicyAsync(sources) ?? []).FirstOrDefault()?.SourceUrl ?? throw new InvalidOperationException("The JSON data blob URL could not be resolved.");

                var responseLength = (await blobApiHttpUtils.Value.ExecuteHttpRequestAsync(() => GetRequest(HttpMethod.Head, blobUri, apiVersion))).Content.Headers.ContentLength ?? 0;
                PipelineBuffer buffer = new() { Length = (int)responseLength, Data = new byte[responseLength], FileName = file?.Name ?? CommandFactory.DefaultTaskDefinitionFile };

                _ = await blobApiHttpUtils.Value.ExecuteHttpRequestAndReadBodyResponseAsync(buffer, () => GetRequest(HttpMethod.Get, blobUri, apiVersion));
                var nodeTask = DeserializeJson(buffer.Data, NodeTaskContext.Default.NodeTask) ?? throw new InvalidOperationException("Failed to deserialize task JSON file.");

                AddDefaultValuesIfMissing(nodeTask);

                if (saveDownload)
                {
                    await parent.SerializeNodeTaskAsync(nodeTask, file ?? new(CommandFactory.DefaultTaskDefinitionFile));
                    file?.Refresh();
                }

                return nodeTask;
            }
            catch (Exception e)
            {
                logger.LogError(e, "Failed to download or deserialize task JSON file.");
                throw;
            }

            static HttpRequestMessage GetRequest(HttpMethod method, Uri uri, string apiVersion)
            {
                HttpRequestMessage request = new(method, uri);
                BlobApiHttpUtils.AddBlobServiceHeaders(request, apiVersion);
                return request;
            }
        }
    }

    private static Lazy<NodeTaskResolverOptions> GetNodeTaskResolverOptions()
    {
        var optionsValue = Environment.GetEnvironmentVariable(nameof(NodeTaskResolverOptions));

        return new(() => string.IsNullOrWhiteSpace(optionsValue)
            ? throw new InvalidOperationException($"Environment variable '{nameof(NodeTaskResolverOptions)}' is required.")
            : DeserializeJson(System.Text.Encoding.UTF8.GetBytes(optionsValue), NodeTaskContext.Default.NodeTaskResolverOptions));
    }

    private static void AddDefaultValuesIfMissing(NodeTask nodeTask)
    {
        nodeTask.RuntimeOptions ??= new();
    }
}
