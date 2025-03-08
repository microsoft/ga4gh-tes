// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.CommandLine;
using System.CommandLine.Parsing;
using CommonUtilities;
using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands
{
    internal static class CommandFactory
    {
        internal const string DefaultTaskDefinitionFile = "runner-task.json";

        private static readonly Uri DefaultDockerUri = new("unix:///var/run/docker.sock");

        internal const string UploadCommandName = "upload";
        internal const string DownloadCommandName = "download";
        internal const string ExecutorCommandName = "exec";
        internal const string StartTaskCommandName = "start-task";
        internal const string DockerUriOption = "docker-url";
        internal const string ExecutorSelectorOption = "executor";

        private static readonly IReadOnlyCollection<Option> GlobalOptions = new List<Option>()
        {
            CreateOption<FileInfo>(BlobPipelineOptionsConverter.FileOption, "The file with the task definition",  "-f", required: true, defaultValue: GetDefaultTaskDefinitionFile()),
            CreateOption<Uri>(BlobPipelineOptionsConverter.UrlOption, $"The URL to download the file with the task definition. This will be saved to the value of '--{BlobPipelineOptionsConverter.FileOption}' if needed. Environment variable 'NodeTaskResolverOptions' must be set.", "-i"),
            CreateOption<string>(BlobPipelineOptionsConverter.ApiVersionOption, "Azure Storage API version", "-v", defaultValue: BlobPipelineOptions.DefaultApiVersion),
        }.AsReadOnly();

        private static readonly IReadOnlyCollection<Option> TransferOptions = new List<Option>()
        {
            CreateOption<int>(BlobPipelineOptionsConverter.BlockSizeOption, "Blob block size in bytes", "-b", defaultValue: BlobSizeUtils.DefaultBlockSizeBytes),
            CreateOption<int>(BlobPipelineOptionsConverter.WritersOption, "Number of concurrent writers", "-w", defaultValue: BlobPipelineOptions.DefaultNumberOfWriters),
            CreateOption<int>(BlobPipelineOptionsConverter.ReadersOption, "Number of concurrent readers", "-r", defaultValue: BlobPipelineOptions.DefaultNumberOfReaders),
            CreateOption<int>(BlobPipelineOptionsConverter.BufferCapacityOption, "Pipeline buffer capacity", "-c", defaultValue: BlobPipelineOptions.DefaultReadWriteBuffersCapacity),
        }.AsReadOnly();

        private static readonly IReadOnlyCollection<Option> ExecOptions = new List<Option>()
        {
            CreateOption<Uri>(DockerUriOption, "Local docker engine endpoint", "-u", defaultValue: DefaultDockerUri),
            CreateOption<int>(ExecutorSelectorOption, "Executor selector", "-e", defaultValue: 0),
        }.AsReadOnly();

        private static void ValidateGlobalOptions(CommandResult commandResult, Command command)
        {
            var urlFound = commandResult.FindResultFor(GetOptionByName<Uri>(command, BlobPipelineOptionsConverter.UrlOption)) is not null;
            var fileResult = commandResult.FindResultFor(GetOptionByName<FileInfo>(command, BlobPipelineOptionsConverter.FileOption));
            var fileValue = fileResult?.IsImplicit ?? true ? GetDefaultTaskDefinitionFile() : fileResult.GetValueOrDefault<FileInfo>();

            commandResult.ErrorMessage = (fileValue?.Exists ?? false, urlFound) switch
            {
                (true, true) => "Option '--file' and '--url' must not both be provided.",
                (false, false) => "Option '--file' or '--url' is required.",
                _ => null,
            };
        }

        internal static RootCommand CreateRootCommand()
        {
            var cmd = new RootCommand("Executes all operations on the node: download, exec and upload");

            GlobalOptions.ForEach(cmd.AddGlobalOption);
            cmd.AddValidator(r => ValidateGlobalOptions(r, cmd));
            TransferOptions.ForEach(cmd.AddOption);
            ExecOptions.ForEach(cmd.AddOption);

            cmd.SetHandler(CommandHandlers.ExecuteRootCommandAsync,
                GetOptionByName<Uri>(cmd, BlobPipelineOptionsConverter.UrlOption),
                GetOptionByName<FileInfo>(cmd, BlobPipelineOptionsConverter.FileOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.BlockSizeOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.WritersOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.ReadersOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.BufferCapacityOption),
                GetOptionByName<string>(cmd, BlobPipelineOptionsConverter.ApiVersionOption),
                GetOptionByName<Uri>(cmd, DockerUriOption));

            return cmd;
        }

        internal static Command CreateUploadCommand(RootCommand rootCommand)
        {
            var cmd = new Command(UploadCommandName, "Uploads output files to blob storage");
            rootCommand.Add(cmd);

            cmd.AddValidator(r => ValidateGlobalOptions(r, cmd));
            TransferOptions.ForEach(cmd.AddOption);

            cmd.SetHandler(CommandHandlers.ExecuteUploadCommandAsync,
                GetOptionByName<Uri>(cmd, BlobPipelineOptionsConverter.UrlOption),
                GetOptionByName<FileInfo>(cmd, BlobPipelineOptionsConverter.FileOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.BlockSizeOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.WritersOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.ReadersOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.BufferCapacityOption),
                GetOptionByName<string>(cmd, BlobPipelineOptionsConverter.ApiVersionOption));

            return cmd;
        }

        internal static Command CreateExecutorCommand(RootCommand rootCommand)
        {
            var cmd = new Command(ExecutorCommandName, "Executes the TES Task commands on the container");
            rootCommand.Add(cmd);

            cmd.AddValidator(r => ValidateGlobalOptions(r, cmd));
            ExecOptions.ForEach(cmd.AddOption);

            cmd.SetHandler(CommandHandlers.ExecuteExecCommandAsync,
                GetOptionByName<Uri>(cmd, BlobPipelineOptionsConverter.UrlOption),
                GetOptionByName<FileInfo>(cmd, BlobPipelineOptionsConverter.FileOption),
                GetOptionByName<string>(cmd, BlobPipelineOptionsConverter.ApiVersionOption),
                GetOptionByName<Uri>(cmd, DockerUriOption),
                GetOptionByName<int>(cmd, ExecutorSelectorOption));

            return cmd;
        }

        internal static Command CreateDownloadCommand(RootCommand rootCommand)
        {
            var cmd = new Command(DownloadCommandName, "Downloads input files from a HTTP source");
            rootCommand.Add(cmd);

            cmd.AddValidator(r => ValidateGlobalOptions(r, cmd));
            TransferOptions.ForEach(cmd.AddOption);

            cmd.SetHandler(CommandHandlers.ExecuteDownloadCommandAsync,
                GetOptionByName<Uri>(cmd, BlobPipelineOptionsConverter.UrlOption),
                GetOptionByName<FileInfo>(cmd, BlobPipelineOptionsConverter.FileOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.BlockSizeOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.WritersOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.ReadersOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.BufferCapacityOption),
                GetOptionByName<string>(cmd, BlobPipelineOptionsConverter.ApiVersionOption));

            return cmd;
        }

        internal static Command CreateStartTaskCommand(RootCommand rootCommand)
        {
            var cmd = new Command(StartTaskCommandName, "");
            rootCommand.Add(cmd);

            cmd.AddValidator(r => ValidateGlobalOptions(r, cmd));
            TransferOptions.ForEach(cmd.AddOption);

            cmd.SetHandler(CommandHandlers.ExecuteStartTaskCommandAsync,
                GetOptionByName<Uri>(cmd, BlobPipelineOptionsConverter.UrlOption),
                GetOptionByName<FileInfo>(cmd, BlobPipelineOptionsConverter.FileOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.BlockSizeOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.WritersOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.ReadersOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.BufferCapacityOption),
                GetOptionByName<string>(cmd, BlobPipelineOptionsConverter.ApiVersionOption));

            return cmd;
        }

        private static FileInfo GetDefaultTaskDefinitionFile()
        {
            return new(DefaultTaskDefinitionFile);
        }

        private static Option<T> GetOptionByName<T>(Command command, string optionName)
        {
            var options = Enumerable.Empty<Option>().Concat(command.Options);

            var option = options.SingleOrDefault(o => o.Name == optionName) as Option<T>;

            if (option is not null)
            {
                return option;
            }

            var result = command.Parents.OfType<Command>()
                .Select(cmd => cmd.Options
                        .Where(IsOptionGlobal)
                        .SingleOrDefault(o => o.Name == optionName)
                    as Option<T>)
                .FirstOrDefault(option => option is not null);

            return result is null
                ? throw new InvalidOperationException("Invalid option (name not found)")
                : result;

            // TODO: Future versions of System.CommandLine are expected to make the flag public, but it appears that it will also be renamed. So we can then get rid of the reflection.
            static bool IsOptionGlobal(Option option)
                => (bool)option.GetType().GetProperty("IsGlobal", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.GetValue(option)!;
        }

        private static Option<T> CreateOption<T>(string name, string description, string alias, bool required = false, object? defaultValue = null, ParseArgument<T>? parse = null, Action<Option<T>>? configureOption = null)
        {
            var option = parse is null
                ? new Option<T>(name: $"--{name}",
                               description: description)
                : new Option<T>(name: $"--{name}",
                               description: description,
                               parseArgument: parse);

            if (defaultValue != null)
            {
                option.SetDefaultValue(defaultValue);
            }

            option.AddAlias(alias);
            option.IsRequired = required;

            configureOption?.Invoke(option);

            return option;
        }
    }
}
