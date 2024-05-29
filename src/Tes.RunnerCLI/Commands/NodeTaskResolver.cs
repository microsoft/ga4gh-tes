// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using Tes.Runner;
using Tes.Runner.Authentication;
using Tes.Runner.Events;
using Tes.Runner.Models;
using Tes.Runner.Storage;
using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands;

[JsonSourceGenerationOptions(PropertyNameCaseInsensitive = true, DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingDefault)]
[JsonSerializable(typeof(NodeTask))]
[JsonSerializable(typeof(NodeTaskResolverOptions))]
public partial class NodeTaskContext : JsonSerializerContext
{ }

public class NodeTaskResolver(Func<RuntimeOptions, string, ResolutionPolicyHandler> resolutionPolicyHandlerFactory, ILogger<NodeTaskResolver> logger, Func<BlobApiHttpUtils>? blobApiHttpUtilsFactory = default)
{
    private readonly ILogger Logger = logger;
    private readonly Lazy<BlobApiHttpUtils> blobApiHttpUtils = new(blobApiHttpUtilsFactory ?? (() => new(logger)));
    private readonly Func<RuntimeOptions, string, ResolutionPolicyHandler> resolutionPolicyHandlerFactory = resolutionPolicyHandlerFactory;

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

        try
        {
            var resolutionPolicy = resolutionPolicyHandlerFactory(options.Value.RuntimeOptions ?? throw new InvalidOperationException($"Environment variable '{nameof(NodeTaskResolverOptions)}' is missing the '{nameof(NodeTaskResolverOptions.RuntimeOptions)}' property."), apiVersion);

            List<FileInput> sources = [new() { TransformationStrategy = options.Value.TransformationStrategy, SourceUrl = uri.AbsoluteUri, Path = (file ?? new(CommandFactory.DefaultTaskDefinitionFile)).FullName }];
            var blobUri = (await resolutionPolicy.ApplyResolutionPolicyAsync(sources) ?? []).FirstOrDefault()?.SourceUrl ?? throw new InvalidOperationException("The JSON data blob URL could not be resolved.");

            var responseLength = (await blobApiHttpUtils.Value.ExecuteHttpRequestAsync(() => GetRequest(HttpMethod.Head, blobUri, apiVersion))).Content.Headers.ContentLength ?? 0;
            PipelineBuffer buffer = new() { Length = (int)responseLength, Data = new byte[responseLength], FileName = file?.Name ?? CommandFactory.DefaultTaskDefinitionFile };

            _ = await blobApiHttpUtils.Value.ExecuteHttpRequestAndReadBodyResponseAsync(buffer, () => GetRequest(HttpMethod.Get, blobUri, apiVersion));
            var nodeTask = DeserializeJson(buffer.Data, NodeTaskContext.Default.NodeTask) ?? throw new InvalidOperationException("Failed to deserialize task JSON file.");

            AddDefaultValuesIfMissing(nodeTask);

            if (saveDownload)
            {
                await SerializeNodeTaskAsync(nodeTask, file ?? new(CommandFactory.DefaultTaskDefinitionFile));
                file?.Refresh();
            }

            return nodeTask;
        }
        catch (Exception e)
        {
            Logger.LogError(e, "Failed to download or deserialize task JSON file.");
            throw;
        }

        static HttpRequestMessage GetRequest(HttpMethod method, Uri uri, string apiVersion)
        {
            HttpRequestMessage request = new(method, uri);
            BlobApiHttpUtils.AddBlobServiceHeaders(request, apiVersion);
            return request;
        }
    }

    private Lazy<NodeTaskResolverOptions> GetNodeTaskResolverOptions()
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

internal static class NodeTaskResolverFactory
{
    internal static readonly Lazy<NodeTaskResolver> NodeTaskResolver = new(() =>
    {
        var loggerFactory = LoggerFactory.Create(Services.ConfigureConsoleLogger);
        return new(ResolutionPolicyHandlerFactoryFactory(loggerFactory), LoggerFactory.Create(Services.ConfigureConsoleLogger).CreateLogger<NodeTaskResolver>());
    });

    private static Func<RuntimeOptions, string, ResolutionPolicyHandler> ResolutionPolicyHandlerFactoryFactory(ILoggerFactory loggerFactory)
        => new((runtimeOptions, apiVersion) =>
        {
            var token = new CredentialsManager(loggerFactory.CreateLogger<CredentialsManager>()).GetTokenCredential(runtimeOptions);

            return new(
                        new(
                            runtimeOptions,
                            apiVersion,
                            new(() => new PassThroughUrlTransformationStrategy()),
                            new(() => new CloudProviderSchemeConverter()),
                            new(() => new ArmUrlTransformationStrategy(token, runtimeOptions, apiVersion, loggerFactory.CreateLogger<ArmUrlTransformationStrategy>())),
                            new(() => new TerraUrlTransformationStrategy(runtimeOptions.Terra!, token, runtimeOptions.AzureEnvironmentConfig!, loggerFactory.CreateLogger<TerraUrlTransformationStrategy>())),
                            new(() => new DrsUriTransformationStrategy(runtimeOptions.Terra!, token, runtimeOptions.AzureEnvironmentConfig!, loggerFactory.CreateLogger<DrsUriTransformationStrategy>()))),
                        sinks => new(sinks, loggerFactory.CreateLogger<EventsPublisher>()));
        });
}
