// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.CommandLine;
using System.CommandLine.Invocation;
using System.CommandLine.Parsing;
using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands
{
    internal class CommandFactory
    {
        internal const string DefaultTaskDefinitionFile = "runner-task.json";

        private static readonly Uri DefaultDockerUri = new("unix:///var/run/docker.sock");

        internal const string UploadCommandName = "upload";
        internal const string DownloadCommandName = "download";
        internal const string ExecutorCommandName = "exec";
        internal const string PreparatoryCommandName = "preparatory";
        internal const string DockerUriOption = "docker-url";

        private static Func<string, string> ShadowCmdName = new(name => name + "-impl");

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

        private static async Task<int> EnsureNodeTask(InvocationContext context)
        {
            var url = context.ParseResult.GetValueForOption(GetOptionByName<Uri>(context.ParseResult.CommandResult.Command, BlobPipelineOptionsConverter.UrlOption));

            if (url is not null)
            {
                var result = await context.ParseResult.RootCommandResult.Command.Subcommands
                    .First(c => PreparatoryCommandName.Equals(c.Name, StringComparison.Ordinal))
                    .Handler!.InvokeAsync(context);

                if (result != 0)
                {
                    return result;
                }

                context.ParseResult
                    .GetValueForOption(GetOptionByName<FileInfo>(context.ParseResult.CommandResult.Command, BlobPipelineOptionsConverter.FileOption))
                    ?.Refresh();
            }

            var cmdName = ShadowCmdName(context.ParseResult.CommandResult.Command.Name);
            return await context.ParseResult.RootCommandResult.Command.Subcommands
                    .First(c => cmdName.Equals(c.Name, StringComparison.Ordinal))
                    .Handler!.InvokeAsync(context);
        }

        private static void ValidateGlobalOption(CommandResult commandResult, Command command)
        {
            var urlFound = commandResult.FindResultFor(GetOptionByName<Uri>(command, BlobPipelineOptionsConverter.UrlOption)) is not null;
            var fileResult = commandResult.FindResultFor(GetOptionByName<FileInfo>(command, BlobPipelineOptionsConverter.FileOption));
            var fileExists = fileResult?.GetValueForOption((Option<FileInfo>)fileResult.Option)?.Exists ?? false;

            commandResult.ErrorMessage = (fileExists, urlFound) switch
            {
                (true, true) => "Option '--file' and '--url' must not both be provided.",
                (false, false) => "Option '--file' or '--url' is required.",
                _ => null,
            };
        }

        internal static RootCommand CreateRootCommand()
        {
            var rootCommand = new RootCommand("Executes all operations on the node: download, exec and upload");
            var preparatoryCommand = new Command(PreparatoryCommandName) { IsHidden = true };
            rootCommand.Add(preparatoryCommand);
            var shadowCmd = new Command(ShadowCmdName(rootCommand.Name)) { IsHidden = true };
            rootCommand.Add(shadowCmd);

            rootCommand.AddOption(CreateOption<Uri>(CommandFactory.DockerUriOption, "local docker engine endpoint", "-u", defaultValue: DefaultDockerUri));

            foreach (var option in GlobalOptions)
            {
                rootCommand.AddGlobalOption(option);
            }

            rootCommand.AddValidator(r => ValidateGlobalOption(r, rootCommand));

            foreach (var option in TransferOptions)
            {
                rootCommand.AddOption(option);
                preparatoryCommand.AddOption(option);
            }

            preparatoryCommand.SetHandler(CommandHandlers.ExecutePreparatoryCommandAsync,
                GetOptionByName<Uri>(preparatoryCommand, BlobPipelineOptionsConverter.UrlOption),
                GetOptionByName<FileInfo>(preparatoryCommand, BlobPipelineOptionsConverter.FileOption),
                GetOptionByName<int>(preparatoryCommand, BlobPipelineOptionsConverter.BlockSizeOption),
                GetOptionByName<int>(preparatoryCommand, BlobPipelineOptionsConverter.WritersOption),
                GetOptionByName<int>(preparatoryCommand, BlobPipelineOptionsConverter.ReadersOption),
                GetOptionByName<int>(preparatoryCommand, BlobPipelineOptionsConverter.BufferCapacityOption),
                GetOptionByName<string>(preparatoryCommand, BlobPipelineOptionsConverter.ApiVersionOption));

            rootCommand.SetHandler(EnsureNodeTask);

            shadowCmd.SetHandler(CommandHandlers.ExecuteRootCommandAsync,
                GetOptionByName<FileInfo>(rootCommand, BlobPipelineOptionsConverter.FileOption),
                GetOptionByName<int>(rootCommand, BlobPipelineOptionsConverter.BlockSizeOption),
                GetOptionByName<int>(rootCommand, BlobPipelineOptionsConverter.WritersOption),
                GetOptionByName<int>(rootCommand, BlobPipelineOptionsConverter.ReadersOption),
                GetOptionByName<int>(rootCommand, BlobPipelineOptionsConverter.BufferCapacityOption),
                GetOptionByName<string>(rootCommand, BlobPipelineOptionsConverter.ApiVersionOption),
                GetOptionByName<Uri>(rootCommand, CommandFactory.DockerUriOption));

            return rootCommand;
        }

        internal static Command CreateUploadCommand(RootCommand rootCommand)
        {
            var cmd = new Command(UploadCommandName, "Uploads output files to blob storage");
            rootCommand.Add(cmd);
            var shadowCmd = new Command(ShadowCmdName(cmd.Name)) { IsHidden = true };
            rootCommand.Add(shadowCmd);
            cmd.AddValidator(r => ValidateGlobalOption(r, cmd));

            foreach (var option in TransferOptions)
            {
                cmd.AddOption(option);
            }

            cmd.SetHandler(EnsureNodeTask);

            shadowCmd.SetHandler(CommandHandlers.ExecuteUploadCommandAsync,
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
            var shadowCmd = new Command(ShadowCmdName(cmd.Name)) { IsHidden = true };
            rootCommand.Add(shadowCmd);
            cmd.AddValidator(r => ValidateGlobalOption(r, cmd));

            cmd.AddOption(CreateOption<Uri>(CommandFactory.DockerUriOption, "local docker engine endpoint", "-u", defaultValue: DefaultDockerUri));

            cmd.SetHandler(EnsureNodeTask);

            shadowCmd.SetHandler(CommandHandlers.ExecuteExecCommandAsync,
                GetOptionByName<FileInfo>(cmd, BlobPipelineOptionsConverter.FileOption),
                GetOptionByName<Uri>(cmd, CommandFactory.DockerUriOption));

            return cmd;
        }

        internal static Command CreateDownloadCommand(RootCommand rootCommand)
        {
            var cmd = new Command(DownloadCommandName, "Downloads input files from a HTTP source");
            rootCommand.Add(cmd);
            var shadowCmd = new Command(ShadowCmdName(cmd.Name)) { IsHidden = true };
            rootCommand.Add(shadowCmd);
            cmd.AddValidator(r => ValidateGlobalOption(r, cmd));

            foreach (var option in TransferOptions)
            {
                cmd.AddOption(option);
            }

            cmd.SetHandler(EnsureNodeTask);

            shadowCmd.SetHandler(CommandHandlers.ExecuteDownloadCommandAsync,
                GetOptionByName<FileInfo>(cmd, BlobPipelineOptionsConverter.FileOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.BlockSizeOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.WritersOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.ReadersOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.BufferCapacityOption),
                GetOptionByName<string>(cmd, BlobPipelineOptionsConverter.ApiVersionOption));

            return cmd;
        }

        private static FileInfo? GetDefaultTaskDefinitionFile()
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
