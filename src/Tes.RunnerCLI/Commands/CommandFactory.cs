using System.CommandLine;
using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands
{
    internal class CommandFactory
    {
        const string DefaultTaskDefinitionFile = "TesTask.json";

        private static readonly Uri DefaultDockerUri = new Uri("unix:///var/run/docker.sock");

        internal const string UploadCommandName = "upload";
        internal const string DownloadCommandName = "download";
        internal const string DockerUriOption = "docker-url";

        internal static RootCommand CreateExecutorCommand()
        {
            var dockerUriOption = CreateOption<Uri>(CommandFactory.DockerUriOption, "local docker engine endpoint", "-u", defaultValue: DefaultDockerUri);

            var rootCommand = new RootCommand("Executes the specified TES Task");

            foreach (var option in CreateGlobalOptionList())
            {
                rootCommand.AddGlobalOption(option);
            }

            rootCommand.AddOption(dockerUriOption);

            rootCommand.SetHandler(CommandHandlers.ExecuteNodeTaskAsync,
                GetOptionByName<FileInfo>(rootCommand.Options, BlobPipelineOptionsConverter.FileOption),
                GetOptionByName<int>(rootCommand.Options, BlobPipelineOptionsConverter.BlockSizeOption),
                GetOptionByName<int>(rootCommand.Options, BlobPipelineOptionsConverter.WritersOption),
                GetOptionByName<int>(rootCommand.Options, BlobPipelineOptionsConverter.ReadersOption),
                GetOptionByName<int>(rootCommand.Options, BlobPipelineOptionsConverter.BufferCapacityOption),
                GetOptionByName<string>(rootCommand.Options, BlobPipelineOptionsConverter.ApiVersionOption),
                dockerUriOption);

            return rootCommand;
        }

        internal static Command CreateUploadCommand(RootCommand rootCommand)
        {
            var cmd = CreateCommand(UploadCommandName, "Uploads output files to blob storage");

            rootCommand.Add(cmd);

            cmd.SetHandler(CommandHandlers.ExecuteUploadTaskAsync,
                GetOptionByName<FileInfo>(cmd.Options, BlobPipelineOptionsConverter.FileOption),
                GetOptionByName<int>(cmd.Options, BlobPipelineOptionsConverter.BlockSizeOption),
                GetOptionByName<int>(cmd.Options, BlobPipelineOptionsConverter.WritersOption),
                GetOptionByName<int>(cmd.Options, BlobPipelineOptionsConverter.ReadersOption),
                GetOptionByName<int>(cmd.Options, BlobPipelineOptionsConverter.BufferCapacityOption),
                GetOptionByName<string>(cmd.Options, BlobPipelineOptionsConverter.ApiVersionOption));

            return cmd;
        }

        internal static Command CreateDownloadCommand(RootCommand rootCommand)
        {
            var cmd = CreateCommand(DownloadCommandName, "Downloads input files from a HTTP source");

            rootCommand.Add(cmd);

            cmd.SetHandler(CommandHandlers.ExecuteDownloadTaskAsync,
                GetOptionByName<FileInfo>(cmd.Options, BlobPipelineOptionsConverter.FileOption),
                GetOptionByName<int>(cmd.Options, BlobPipelineOptionsConverter.BlockSizeOption),
                GetOptionByName<int>(cmd.Options, BlobPipelineOptionsConverter.WritersOption),
                GetOptionByName<int>(cmd.Options, BlobPipelineOptionsConverter.ReadersOption),
                GetOptionByName<int>(cmd.Options, BlobPipelineOptionsConverter.BufferCapacityOption),
                GetOptionByName<string>(cmd.Options, BlobPipelineOptionsConverter.ApiVersionOption));

            return cmd;
        }

        internal static Command CreateCommand(string optName, string optDescription, params Option[] options)
        {
            var cmd = new Command(optName, optDescription);

            var allOptions = CreateGlobalOptionList();

            allOptions.AddRange(options);

            foreach (var option in allOptions)
            {
                cmd.AddOption(option);
            }

            return cmd;
        }

        private static List<Option> CreateGlobalOptionList()
        {
            return new List<Option>()
            {
                CreateOption<FileInfo>(BlobPipelineOptionsConverter.FileOption, "The file with the task definition",  "-f", required: true, defaultValue: GetDefaultTaskDefinitionFile()),
                CreateOption<int>(BlobPipelineOptionsConverter.BlockSizeOption, "Blob block size in bytes", "-b", defaultValue: BlobSizeUtils.DefaultBlockSizeBytes),
                CreateOption<int>(BlobPipelineOptionsConverter.WritersOption, "Number of concurrent writers", "-w", defaultValue: BlobPipelineOptions.DefaultNumberOfWriters),
                CreateOption<int>(BlobPipelineOptionsConverter.ReadersOption, "Number of concurrent readers", "-r", defaultValue: BlobPipelineOptions.DefaultNumberOfReaders),
                CreateOption<int>(BlobPipelineOptionsConverter.BufferCapacityOption, "Pipeline buffer capacity", "-c", defaultValue: BlobPipelineOptions.DefaultReadWriteBuffersCapacity),
                CreateOption<string>(BlobPipelineOptionsConverter.ApiVersionOption, "Azure Storage API version", "-v", defaultValue: BlobPipelineOptions.DefaultApiVersion),
            };
        }

        private static FileInfo GetDefaultTaskDefinitionFile()
        {
            return new FileInfo(DefaultTaskDefinitionFile);
        }

        private static Option<T> GetOptionByName<T>(IReadOnlyCollection<Option> commandOptions, string optionName)
        {
            var option = commandOptions.SingleOrDefault(o => o.Name == optionName);

            if (option != null)
            {
                return (Option<T>)option;
            }

            throw new InvalidOperationException("Invalid option");
        }

        private static Option<T> CreateOption<T>(string name, string description, string alias, bool required = false, object? defaultValue = null)
        {
            var option = new Option<T>(
                                name: $"--{name}",
                               description: description);

            if (defaultValue != null)
            {
                option.SetDefaultValue(defaultValue);
            }

            option.AddAlias(alias);
            option.IsRequired = required;

            return option;
        }
    }
}
