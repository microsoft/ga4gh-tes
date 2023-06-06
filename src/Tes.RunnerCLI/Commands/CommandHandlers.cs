// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json;
using Microsoft.Extensions.Logging;
using Tes.Runner;
using Tes.Runner.Docker;
using Tes.Runner.Models;
using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands
{
    internal class CommandHandlers
    {
        private static readonly ILogger Logger = PipelineLoggerFactory.Create<CommandHandlers>();
        private const int SuccessExitCode = 0;
        private const int ErrorExitCode = 1;

        internal static async Task<int> ExecuteNodeTaskAsync(FileInfo file,
            int blockSize,
            int writers,
            int readers,
            int bufferCapacity,
            string apiVersion,
            Uri dockerUri)
        {
            try
            {
                var options = BlobPipelineOptionsConverter.ToBlobPipelineOptions(blockSize, writers, readers, bufferCapacity, apiVersion);

                await ExecuteTransferAsSubProcessAsync(CommandFactory.DownloadCommandName, file, options);

                await ExecuteNodeContainerTaskAsync(file, dockerUri, options);

                await ExecuteTransferAsSubProcessAsync(CommandFactory.UploadCommandName, file, options);

                return SuccessExitCode;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Failed to execute the task. Error: {e.Message}");
                Logger.LogError(e, "Failed to execute the task");
                return ErrorExitCode;
            }
        }

        private static async Task ExecuteNodeContainerTaskAsync(FileInfo file, Uri dockerUri, BlobPipelineOptions options)
        {
            try
            {
                var nodeTask = DeserializeNodeTask(file.FullName);

                var executor = new Executor(nodeTask, options);

                var result = await executor.ExecuteNodeContainerTaskAsync(new DockerExecutor(dockerUri));

                if (result is null)
                {
                    throw new InvalidOperationException("The container task failed to return results");
                }

                var (stdout, stderr) = await result.ContainerResult.Logs.ReadOutputToEndAsync(CancellationToken.None);

                Console.WriteLine($"Execution Status Code: {result.ContainerResult.StatusCode}. Error: {result.ContainerResult.Error}");
                Console.WriteLine($"StdOutput: {stdout}");
                Console.WriteLine($"StdError: {stderr}");
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Failed to execute the task.");
                throw;
            }
        }

        private static BlobPipelineOptions CreateBlobPipelineOptions(int blockSize, int writers, int readers,
            int bufferCapacity, string apiVersion)
        {
            var options = new BlobPipelineOptions(
                BlockSizeBytes: blockSize,
                NumberOfWriters: writers,
                NumberOfReaders: readers,
                ReadWriteBuffersCapacity: bufferCapacity,
                MemoryBufferCapacity: bufferCapacity,
                ApiVersion: apiVersion);

            return options;
        }

        internal static async Task<int> ExecuteUploadTaskAsync(FileInfo file,
            int blockSize,
            int writers,
            int readers,
            int bufferCapacity,
            string apiVersion)
        {
            var options = CreateBlobPipelineOptions(blockSize, writers, readers, bufferCapacity, apiVersion);

            Console.WriteLine("Starting upload operation.");

            return await ExecuteTransferTaskAsync(file, options, exec => exec.UploadOutputsAsync(), ExpandOutputs);
        }

        private static void HandleResult(ProcessExecutionResult results, string command)
        {
            if (results.ExitCode != 0)
            {
                throw new Exception(
                    $"Task operation failed. Command: {command}. Exit Code: {results.ExitCode}{Environment.NewLine}Error: {results.StandardError}{Environment.NewLine}Output: {results.StandardOutput}");
            }

            Console.WriteLine($"Result: {results.StandardOutput}");
        }

        private static async Task ExecuteTransferAsSubProcessAsync(string command, FileInfo file, BlobPipelineOptions options)
        {
            var processLauncher = new ProcessLauncher();

            var results = await processLauncher.LaunchProcessAndWaitAsync(BlobPipelineOptionsConverter.ToCommandArgs(command, file.FullName, options));

            HandleResult(results, command);
        }

        internal static async Task<int> ExecuteDownloadTaskAsync(FileInfo file,
            int blockSize,
            int writers,
            int readers,
            int bufferCapacity,
            string apiVersion)
        {
            var options = CreateBlobPipelineOptions(blockSize, writers, readers, bufferCapacity, apiVersion);

            Console.WriteLine("Starting download operation.");

            return await ExecuteTransferTaskAsync(file, options, exec => exec.DownloadInputsAsync(), ExpandInputs);
        }

        private static async Task<int> ExecuteTransferTaskAsync(FileInfo taskDefinitionFile, BlobPipelineOptions options, Func<Executor, Task<long>> transferOperation, Action<NodeTask> expandFileSpecs)
        {
            try
            {
                var nodeTask = DeserializeNodeTask(taskDefinitionFile.FullName);

                expandFileSpecs(nodeTask);

                var executor = new Executor(nodeTask, options);

                var result = await transferOperation(executor);

                Console.WriteLine($"Total bytes transfer: {result:n0}");

                return SuccessExitCode;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Failed to perform transfer. Error: {e.Message} Operation: {transferOperation.Method.Name}");
                Logger.LogError(e, $"Failed to perform transfer. Operation: {transferOperation}");
                return ErrorExitCode;
            }
        }

        private static void ExpandInputs(NodeTask nodeTask)
        {
            var inputs = new List<FileInput>();

            foreach (var input in nodeTask.Inputs ?? Enumerable.Empty<FileInput>())
            {
                inputs.Add(new FileInput
                {
                    SasStrategy = input.SasStrategy,
                    SourceUrl = input.SourceUrl,
                    FullFileName = ExpandEnvironmentVariables(input.FullFileName!)
                });
            }

            nodeTask.Inputs = inputs;
        }

        private static void ExpandOutputs(NodeTask nodeTask)
        {
            var outputs = new List<FileOutput>();

            foreach (var output in nodeTask.Outputs ?? Enumerable.Empty<FileOutput>())
            {
                outputs.AddRange(ExpandOutput(output));
            }

            nodeTask.Outputs = outputs;
        }

        private static IEnumerable<FileOutput> ExpandOutput(FileOutput output)
        {
            ArgumentNullException.ThrowIfNull(output);

            switch (output.FileType)
            {
                case FileType.File:
                    return Enumerable.Empty<FileOutput>().Append(new()
                    {
                        Required = output.Required,
                        SasStrategy = output.SasStrategy,
                        TargetUrl = output.TargetUrl,
                        FullFileName = ExpandEnvironmentVariables(output.FullFileName!),
                    });

                case FileType.Directory:
                    {
                        var dir = new DirectoryInfo(ExpandEnvironmentVariables(output.FullFileName!));

                        if (dir.Exists)
                        {
                            return ExpandDirectoryContents(dir).Select(GetFileOutput);
                        }
                        else if (output.Required.GetValueOrDefault())
                        {
                            throw new DirectoryNotFoundException($"Directory '{output.FullFileName}' was not found");
                        }

                        return Enumerable.Empty<FileOutput>();

                        FileOutput GetFileOutput(FileInfo file)
                        {
                            return new FileOutput { Required = output.Required, SasStrategy = output.SasStrategy, FullFileName = file.FullName, TargetUrl = ExpandUrl(output.TargetUrl!, dir!.FullName, file.FullName) };
                        }
                    }

                default:
                    throw new ArgumentException("Invalid FileType for output file.", nameof(output));
            }

        }

        private static IEnumerable<FileInfo> ExpandDirectoryContents(DirectoryInfo dir)
        {
            var result = Enumerable.Empty<FileInfo>();

            foreach (var entry in dir.EnumerateFileSystemInfos())
            {
                switch (entry)
                {
                    case FileInfo file:
                        result = result.Append(file);
                        break;

                    case DirectoryInfo directory:
                        result = result.Concat(ExpandDirectoryContents(directory));
                        break;
                }
            }

            return result;
        }

        private static string ExpandUrl(string url, string dir, string path)
        {
            var uri = new Uri(url);
            var relativePath = Path.GetRelativePath(dir, path);
            return new Uri(uri, $"{uri.AbsolutePath}/{relativePath}{uri.Query}").ToString();
        }

        private static string ExpandEnvironmentVariables(string fullFileName)
        {
            return Environment.ExpandEnvironmentVariables(fullFileName);
        }

        private static NodeTask DeserializeNodeTask(string tesNodeTaskFilePath)
        {
            try
            {
                var nodeTask = File.ReadAllText(tesNodeTaskFilePath);

                return JsonSerializer.Deserialize<NodeTask>(nodeTask, new JsonSerializerOptions() { PropertyNameCaseInsensitive = true }) ?? throw new InvalidOperationException("The JSON data provided is invalid.");
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Failed to deserialize task JSON file.");
                throw;
            }
        }
    }
}
