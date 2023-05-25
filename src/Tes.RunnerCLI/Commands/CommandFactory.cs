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
            CreateOption<bool>(BlobPipelineOptionsConverter.SkipMissingSources, "Skip missing sources for file transfers", "-s", defaultValue: BlobPipelineOptions.DefaultSkipMissingSources), // TODO: remove this options when we remove the node task script from BatchScheduler
            CreateOption<int>(BlobPipelineOptionsConverter.BufferCapacityOption, "Pipeline buffer capacity", "-c", defaultValue: BlobPipelineOptions.DefaultReadWriteBuffersCapacity),
            CreateOption<string>(BlobPipelineOptionsConverter.ApiVersionOption, "Azure Storage API version", "-v", defaultValue: BlobPipelineOptions.DefaultApiVersion),
        }.AsReadOnly();

        internal static RootCommand CreateExecutorCommand()
        {
            var dockerUriOption = CreateOption<Uri>(CommandFactory.DockerUriOption, "local docker engine endpoint", "-u", defaultValue: DefaultDockerUri);

            var rootCommand = new RootCommand("Executes the specified TES Task");

            foreach (var option in GlobalOptions)
            {
                rootCommand.AddGlobalOption(option);
            }

            rootCommand.AddOption(dockerUriOption);

            // GetOptionByName() expects that the "globalOptions" parameter contain only inherited global options. Thus, we don't pass any since we're the root.
            var fakedGlobalList = Enumerable.Empty<Option>().ToList();

            rootCommand.SetHandler(CommandHandlers.ExecuteNodeTaskAsync,
                GetOptionByName<FileInfo>(rootCommand.Options, fakedGlobalList, BlobPipelineOptionsConverter.FileOption),
                GetOptionByName<int>(rootCommand.Options, fakedGlobalList, BlobPipelineOptionsConverter.BlockSizeOption),
                GetOptionByName<int>(rootCommand.Options, fakedGlobalList, BlobPipelineOptionsConverter.WritersOption),
                GetOptionByName<int>(rootCommand.Options, fakedGlobalList, BlobPipelineOptionsConverter.ReadersOption),
                GetOptionByName<bool>(rootCommand.Options, fakedGlobalList, BlobPipelineOptionsConverter.SkipMissingSources),
                GetOptionByName<int>(rootCommand.Options, fakedGlobalList, BlobPipelineOptionsConverter.BufferCapacityOption),
                GetOptionByName<string>(rootCommand.Options, fakedGlobalList, BlobPipelineOptionsConverter.ApiVersionOption),
                GetOptionByName<Uri>(rootCommand.Options, fakedGlobalList, CommandFactory.DockerUriOption));

            return rootCommand;
        }

        internal static Command CreateUploadCommand(RootCommand rootCommand)
        {
            var cmd = CreateCommand(UploadCommandName, "Uploads output files to blob storage");

            rootCommand.Add(cmd);

            cmd.SetHandler(CommandHandlers.ExecuteUploadTaskAsync,
                GetOptionByName<FileInfo>(cmd.Options, GlobalOptions, BlobPipelineOptionsConverter.FileOption),
                GetOptionByName<int>(cmd.Options, GlobalOptions, BlobPipelineOptionsConverter.BlockSizeOption),
                GetOptionByName<int>(cmd.Options, GlobalOptions, BlobPipelineOptionsConverter.WritersOption),
                GetOptionByName<int>(cmd.Options, GlobalOptions, BlobPipelineOptionsConverter.ReadersOption),
                GetOptionByName<bool>(cmd.Options, GlobalOptions, BlobPipelineOptionsConverter.SkipMissingSources),
                GetOptionByName<int>(cmd.Options, GlobalOptions, BlobPipelineOptionsConverter.BufferCapacityOption),
                GetOptionByName<string>(cmd.Options, GlobalOptions, BlobPipelineOptionsConverter.ApiVersionOption));

            return cmd;
        }

        internal static Command CreateDownloadCommand(RootCommand rootCommand)
        {
            var cmd = CreateCommand(DownloadCommandName, "Downloads input files from a HTTP source");

            rootCommand.Add(cmd);

            cmd.SetHandler(CommandHandlers.ExecuteDownloadTaskAsync,
                GetOptionByName<FileInfo>(cmd.Options, GlobalOptions, BlobPipelineOptionsConverter.FileOption),
                GetOptionByName<int>(cmd.Options, GlobalOptions, BlobPipelineOptionsConverter.BlockSizeOption),
                GetOptionByName<int>(cmd.Options, GlobalOptions, BlobPipelineOptionsConverter.WritersOption),
                GetOptionByName<int>(cmd.Options, GlobalOptions, BlobPipelineOptionsConverter.ReadersOption),
                GetOptionByName<bool>(cmd.Options, GlobalOptions, BlobPipelineOptionsConverter.SkipMissingSources),
                GetOptionByName<int>(cmd.Options, GlobalOptions, BlobPipelineOptionsConverter.BufferCapacityOption),
                GetOptionByName<string>(cmd.Options, GlobalOptions, BlobPipelineOptionsConverter.ApiVersionOption));

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

        private static Option<T> GetOptionByName<T>(IReadOnlyCollection<Option> commandOptions, IReadOnlyCollection<Option> globalOptions, string optionName)
        {
            var option = commandOptions.Concat(globalOptions).SingleOrDefault(o => o.Name == optionName);

            if (option is not null)
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
