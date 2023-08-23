// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.CommandLine;
using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands
{
    internal class CommandFactory
    {
        const string DefaultTaskDefinitionFile = "TesTask.json";

        private static readonly Uri DefaultDockerUri = new("unix:///var/run/docker.sock");

        internal const string UploadCommandName = "upload";
        internal const string DownloadCommandName = "download";
        internal const string DockerUriOption = "docker-url";

        private static readonly IReadOnlyCollection<Option> GlobalOptions = new List<Option>()
        {
            CreateOption<FileInfo>(BlobPipelineOptionsConverter.FileOption, "The file with the task definition",  "-f", required: true, defaultValue: GetDefaultTaskDefinitionFile()),
            CreateOption<int>(BlobPipelineOptionsConverter.BlockSizeOption, "Blob block size in bytes", "-b", defaultValue: BlobSizeUtils.DefaultBlockSizeBytes),
            CreateOption<int>(BlobPipelineOptionsConverter.WritersOption, "Number of concurrent writers", "-w", defaultValue: BlobPipelineOptions.DefaultNumberOfWriters),
            CreateOption<int>(BlobPipelineOptionsConverter.ReadersOption, "Number of concurrent readers", "-r", defaultValue: BlobPipelineOptions.DefaultNumberOfReaders),
            CreateOption<int>(BlobPipelineOptionsConverter.BufferCapacityOption, "Pipeline buffer capacity", "-c", defaultValue: BlobPipelineOptions.DefaultReadWriteBuffersCapacity),
            CreateOption<string>(BlobPipelineOptionsConverter.ApiVersionOption, "Azure Storage API version", "-v", defaultValue: BlobPipelineOptions.DefaultApiVersion)
        }.AsReadOnly();

        internal static RootCommand CreateExecutorCommand()
        {
            var rootCommand = new RootCommand("Executes the specified TES Task");

            rootCommand.AddOption(CreateOption<Uri>(CommandFactory.DockerUriOption, "local docker engine endpoint", "-u", defaultValue: DefaultDockerUri));

            foreach (var option in GlobalOptions)
            {
                rootCommand.AddGlobalOption(option);
            }

            rootCommand.SetHandler(CommandHandlers.ExecuteNodeTaskAsync,
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
            var cmd = CreateCommand(UploadCommandName, "Uploads output files to blob storage");
            rootCommand.Add(cmd);

            cmd.SetHandler(CommandHandlers.ExecuteUploadTaskAsync,
                GetOptionByName<FileInfo>(cmd, BlobPipelineOptionsConverter.FileOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.BlockSizeOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.WritersOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.ReadersOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.BufferCapacityOption),
                GetOptionByName<string>(cmd, BlobPipelineOptionsConverter.ApiVersionOption));

            return cmd;
        }

        internal static Command CreateDownloadCommand(RootCommand rootCommand)
        {
            var cmd = CreateCommand(DownloadCommandName, "Downloads input files from a HTTP source");
            rootCommand.Add(cmd);

            cmd.SetHandler(CommandHandlers.ExecuteDownloadTaskAsync,
                GetOptionByName<FileInfo>(cmd, BlobPipelineOptionsConverter.FileOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.BlockSizeOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.WritersOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.ReadersOption),
                GetOptionByName<int>(cmd, BlobPipelineOptionsConverter.BufferCapacityOption),
                GetOptionByName<string>(cmd, BlobPipelineOptionsConverter.ApiVersionOption));

            return cmd;
        }

        internal static Command CreateCommand(string optName, string optDescription, params Option[] options)
        {
            var cmd = new Command(optName, optDescription);

            foreach (var option in options)
            {
                cmd.AddOption(option);
            }

            return cmd;
        }

        private static FileInfo GetDefaultTaskDefinitionFile()
        {
            return new FileInfo(DefaultTaskDefinitionFile);
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

            configureOption?.Invoke(option!);

            return option;
        }
    }
}
