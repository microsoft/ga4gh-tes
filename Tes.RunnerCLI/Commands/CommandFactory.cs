using System.CommandLine;
using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands
{
    internal class CommandFactory
    {
        const string FileOption = "file";
        const string BlockSizeOption = "blockSize";
        const string WritersOption = "writers";
        const string ReadersOption = "readers";
        const string BufferCapacityOption = "bufferCapacity";
        const string ApiVersionOption = "apiVersion";
        const string DockerUriOption = "docker-url";
        private static readonly Uri DefaultDockerUri = new Uri("unix:///var/run/docker.sock");

        const string ExecCommandName = "exec";
        private const string UploadCommandName = "upload";
        private const string DownloadCommandName = "download";

        internal static Command CreateExecutorCommand()
        {

            var dockerUriOption = CreateOption<Uri>(DockerUriOption, "local docker engine endpoint", "-u", defaultValue: DefaultDockerUri);

            var cmd = CreateCommand(ExecCommandName, "Downloads input files, executes specified commands and uploads output files", dockerUriOption);

            cmd.SetHandler(async (file, blockSize, writers, readers, bufferCapacity, apiVersion, dockerUri) =>
                {
                    await CommandHandlers.ExecuteNodeTaskAsync(file, blockSize, writers, readers, bufferCapacity, apiVersion, dockerUri);
                },
                GetOptionByName<FileInfo>(cmd.Options, FileOption),
                GetOptionByName<int>(cmd.Options, BlockSizeOption),
                GetOptionByName<int>(cmd.Options, WritersOption),
                GetOptionByName<int>(cmd.Options, ReadersOption),
                GetOptionByName<int>(cmd.Options, BufferCapacityOption),
                GetOptionByName<string>(cmd.Options, ApiVersionOption),
                dockerUriOption);

            return cmd;
        }

        internal static Command CreateUploadCommand()
        {
            var cmd = CreateCommand(UploadCommandName, "Uploads output files to blob storage");

            cmd.SetHandler(async (file, blockSize, writers, readers, bufferCapacity, apiVersion) =>
                {
                    await CommandHandlers.ExecuteUploadTaskAsync(file,
                            blockSize,
                            writers,
                            readers,
                            bufferCapacity,
                            apiVersion);
                },
                GetOptionByName<FileInfo>(cmd.Options, FileOption),
                GetOptionByName<int>(cmd.Options, BlockSizeOption),
                GetOptionByName<int>(cmd.Options, WritersOption),
                GetOptionByName<int>(cmd.Options, ReadersOption),
                GetOptionByName<int>(cmd.Options, BufferCapacityOption),
                GetOptionByName<string>(cmd.Options, ApiVersionOption));

            return cmd;
        }

        internal static Command CreateDownloadCommand()
        {

            var cmd = CreateCommand(DownloadCommandName, "Downloads input files from a HTTP source");

            cmd.SetHandler(async (file, blockSize, writers, readers, bufferCapacity, apiVersion) =>
                {
                    await CommandHandlers.ExecuteDownloadTaskAsync(file,
                        blockSize,
                        writers,
                        readers,
                        bufferCapacity,
                        apiVersion);

                },
                GetOptionByName<FileInfo>(cmd.Options, FileOption),
                GetOptionByName<int>(cmd.Options, BlockSizeOption),
                GetOptionByName<int>(cmd.Options, WritersOption),
                GetOptionByName<int>(cmd.Options, ReadersOption),
                GetOptionByName<int>(cmd.Options, BufferCapacityOption),
                GetOptionByName<string>(cmd.Options, ApiVersionOption));

            return cmd;
        }

        internal static Command CreateCommand(string optName, string optDescription, params Option[] options)
        {

            var cmd = new Command(optName, optDescription);

            var allOptions = CreateBaseOptionList();

            allOptions.AddRange(options);

            foreach (var option in allOptions)
            {
                cmd.AddOption(option);
            }

            return cmd;
        }


        private static List<Option> CreateBaseOptionList()
        {

            return new List<Option>()
            {
                CreateOption<FileInfo>(FileOption, "The file with the task definition",  "-f", required: true),
                CreateOption<int>(BlockSizeOption, "Blob block size in bytes", "-b", defaultValue: BlobSizeUtils.DefaultBlockSize),
                CreateOption<int>(WritersOption, "Number of concurrent writers", "-w", defaultValue: BlobPipelineOptions.DefaultNumberOfWriters),
                CreateOption<int>(ReadersOption, "Number of concurrent readers", "-r", defaultValue: BlobPipelineOptions.DefaultNumberOfReaders),
                CreateOption<int>(BufferCapacityOption, "Pipeline buffer capacity", "-c", defaultValue: BlobPipelineOptions.DefaultReadWriteBuffersCapacity),
                CreateOption<string>(ApiVersionOption, "Azure Storage API version", "-v", defaultValue: BlobPipelineOptions.DefaultApiVersion),
            };
        }

        private static Option<T> GetOptionByName<T>(IReadOnlyCollection<Option> commandOptions, string optionName)
        {
            var option = commandOptions.SingleOrDefault(o => o.Name == optionName);

            return (Option<T>)option;
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
