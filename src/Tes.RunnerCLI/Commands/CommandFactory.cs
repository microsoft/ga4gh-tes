// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.CommandLine;
using System.CommandLine.Parsing;
using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands
{
    internal class CommandFactory(CommandHandlers? handlers = default)
    {
        private readonly CommandHandlers CommandHandlers = handlers ?? CommandHandlers.Instance;
        internal static CommandFactory Instance => SingletonFactory.Value;
        private static readonly Lazy<CommandFactory> SingletonFactory = new(() => new());

        internal const string DefaultTaskDefinitionFile = "runner-task.json";

        private static readonly Uri DefaultDockerUri = new("unix:///var/run/docker.sock");

        internal const string UploadCommandName = "upload";
        internal const string DownloadCommandName = "download";
        internal const string ExecutorCommandName = "exec";
        internal const string DockerUriOption = "docker-url";

        private static readonly IReadOnlyCollection<Option> GlobalOptions = new List<Option>()
        {
            CreateOption<FileInfo>(BlobPipelineOptionsConverter.FileOption, "The file with the task definition",  "-f", required: true, defaultValue: GetDefaultTaskDefinitionFile()),
            CreateOption<Uri>(BlobPipelineOptionsConverter.UrlOption, $"The URL to download the file with the task definition. This will be saved as the value of '--{BlobPipelineOptionsConverter.FileOption}'", "-i")
        }.AsReadOnly();

        private static readonly IReadOnlyCollection<Option> TransferOptions = new List<Option>()
        {
            CreateOption<int>(BlobPipelineOptionsConverter.BlockSizeOption, "Blob block size in bytes", "-b", defaultValue: BlobSizeUtils.DefaultBlockSizeBytes),
            CreateOption<int>(BlobPipelineOptionsConverter.WritersOption, "Number of concurrent writers", "-w", defaultValue: BlobPipelineOptions.DefaultNumberOfWriters),
            CreateOption<int>(BlobPipelineOptionsConverter.ReadersOption, "Number of concurrent readers", "-r", defaultValue: BlobPipelineOptions.DefaultNumberOfReaders),
            CreateOption<int>(BlobPipelineOptionsConverter.BufferCapacityOption, "Pipeline buffer capacity", "-c", defaultValue: BlobPipelineOptions.DefaultReadWriteBuffersCapacity),
            CreateOption<string>(BlobPipelineOptionsConverter.ApiVersionOption, "Azure Storage API version", "-v", defaultValue: BlobPipelineOptions.DefaultApiVersion)
        }.AsReadOnly();

        private static void ValidateGlobalOption(CommandResult commandResult, Command command)
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

        internal RootCommand CreateRootCommand()
        {
            var rootCommand = new RootCommand("Executes all operations on the node: download, exec and upload");

            rootCommand.AddOption(CreateOption<Uri>(CommandFactory.DockerUriOption, "local docker engine endpoint", "-u", defaultValue: DefaultDockerUri));

            foreach (var option in GlobalOptions)
            {
                rootCommand.AddGlobalOption(option);
            }

            rootCommand.AddValidator(r => ValidateGlobalOption(r, rootCommand));

            foreach (var option in TransferOptions)
            {
                rootCommand.AddOption(option);
            }

            rootCommand.SetHandler(CommandHandlers.ExecuteRootCommandAsync,
                GetOptionByName<Uri>(rootCommand, BlobPipelineOptionsConverter.UrlOption),
                GetOptionByName<FileInfo>(rootCommand, BlobPipelineOptionsConverter.FileOption),
                GetOptionByName<int>(rootCommand, BlobPipelineOptionsConverter.BlockSizeOption),
                GetOptionByName<int>(rootCommand, BlobPipelineOptionsConverter.WritersOption),
                GetOptionByName<int>(rootCommand, BlobPipelineOptionsConverter.ReadersOption),
                GetOptionByName<int>(rootCommand, BlobPipelineOptionsConverter.BufferCapacityOption),
                GetOptionByName<string>(rootCommand, BlobPipelineOptionsConverter.ApiVersionOption),
                GetOptionByName<Uri>(rootCommand, CommandFactory.DockerUriOption));

            return rootCommand;
        }

        internal Command CreateUploadCommand(RootCommand rootCommand)
        {
            var cmd = new Command(UploadCommandName, "Uploads output files to blob storage");
            rootCommand.Add(cmd);

            foreach (var option in TransferOptions)
            {
                cmd.AddOption(option);
            }

            cmd.SetHandler(CommandHandlers.ExecuteUploadCommandAsync,
                GetOptionByName<FileInfo>(cmd, BlobPipelineOptionsConverter.FileOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.BlockSizeOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.WritersOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.ReadersOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.BufferCapacityOption),
                GetOptionByName<string>(cmd, BlobPipelineOptionsConverter.ApiVersionOption));

            return cmd;
        }

        internal Command CreateExecutorCommand(RootCommand rootCommand)
        {
            var cmd = new Command(ExecutorCommandName, "Executes the TES Task commands on the container");
            rootCommand.Add(cmd);

            cmd.AddOption(CreateOption<Uri>(CommandFactory.DockerUriOption, "local docker engine endpoint", "-u", defaultValue: DefaultDockerUri));

            cmd.SetHandler(CommandHandlers.ExecuteExecCommandAsync,
                GetOptionByName<FileInfo>(cmd, BlobPipelineOptionsConverter.FileOption),
                GetOptionByName<Uri>(cmd, CommandFactory.DockerUriOption));

            return cmd;
        }

        internal Command CreateDownloadCommand(RootCommand rootCommand)
        {
            var cmd = new Command(DownloadCommandName, "Downloads input files from a HTTP source");
            rootCommand.Add(cmd);

            foreach (var option in TransferOptions)
            {
                cmd.AddOption(option);
            }

            cmd.SetHandler(CommandHandlers.ExecuteDownloadCommandAsync,
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

            foreach (var cmd in command.Parents.OfType<Command>())
            {
                option = cmd.Options.Where(IsOptionGlobal).SingleOrDefault(o => o.Name == optionName) as Option<T>;

                if (option is not null)
                {
                    return option;
                }
            }

            throw new InvalidOperationException("Invalid option (name not found)");

            // TODO: Future versions of System.CommandLine are expected to make the flag public, but it appears that it will also be renamed. So we can then get rid of the reflection.
            static bool IsOptionGlobal(Option option)
                => (bool)option.GetType().GetProperty("IsGlobal", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.GetValue(option)!;
        }

        private static Option<T> CreateOption<T>(string name, string description, string alias, bool required = false, object? defaultValue = null, System.CommandLine.Parsing.ParseArgument<T>? parse = null, Action<Option<T>>? configureOption = null)
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
