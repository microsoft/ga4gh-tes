using System.CommandLine;
using Tes.Runner.Transfer;

namespace Tes.RunnerCLI.Commands
{
    internal class CommandFactory
    {
        internal static Command CreateExecutorCommand()
        {

            var dockerUriOption = new Option<Uri?>(
                name: "--docker-url",
                description: "local docker engine endpoint",
                getDefaultValue: () => new Uri("unix:///var/run/docker.sock")
            );

            var cmd = CreateCommand("exec", "Downloads input files, executes specified commands and uploads output files", dockerUriOption);

            cmd.SetHandler((file, blockSize, writers, readers, apiVersion, dockerUri) =>
                CommandHandlers.ExecuteNodeTaskAsync(file,
                    blockSize,
                    writers,
                    readers,
                    apiVersion,
                    dockerUri).Wait(),
                (Option<FileInfo>)cmd.Options[0],
                (Option<int>)cmd.Options[1],
                (Option<int>)cmd.Options[2],
                (Option<int>)cmd.Options[3],
                (Option<string>)cmd.Options[4],
                (Option<Uri>)cmd.Options[5]);

            return cmd;
        }

        internal static Command CreateUploadCommand()
        {
            var cmd = CreateCommand("upload", "Uploads output files to blob storage");

            cmd.SetHandler((file, blockSize, writers, readers, apiVersion) =>
                    CommandHandlers.ExecuteUploadTaskAsync(file,
                        blockSize,
                        writers,
                        readers,
                        apiVersion).Wait(),
                (Option<FileInfo>)cmd.Options[0],
                (Option<int>)cmd.Options[1],
                (Option<int>)cmd.Options[2],
                (Option<int>)cmd.Options[3],
                (Option<string>)cmd.Options[4]);

            return cmd;
        }

        internal static Command CreateDownloadCommand()
        {

            var cmd = CreateCommand("download", "Downloads input files from a HTTP source");

            cmd.SetHandler((file, blockSize, writers, readers, apiVersion) =>
                    CommandHandlers.ExecuteDownloadTaskAsync(file,
                        blockSize,
                        writers,
                        readers,
                        apiVersion).Wait(),
                (Option<FileInfo>)cmd.Options[0],
                (Option<int>)cmd.Options[1],
                (Option<int>)cmd.Options[2],
                (Option<int>)cmd.Options[3],
                (Option<string>)cmd.Options[4]);

            return cmd;
        }

        internal static Command CreateCommand(string optName, string optDescription, params Option[] options)
        {
            var fileOption = new Option<FileInfo>(
                name: "--file",
                description: "The file with the task definition.");

            var blockSize = new Option<int>(
                name: "--blockSize",
                description: "Blob block size.",
                getDefaultValue: () => BlobSizeUtils.DefaultBlockSize);

            var writers = new Option<int>(
                name: "--writers",
                description: "Number of concurrent writers",
                getDefaultValue: () => BlobPipelineOptions.DefaultNumberOfWriters);

            var readers = new Option<int>(
                name: "--readers",
                description: "Number of concurrent readers",
                getDefaultValue: () => BlobPipelineOptions.DefaultNumberOfWriters);

            var apiVersion = new Option<string>(
                name: "--apiVersion",
                description: "Azure Storage API version",
                getDefaultValue: () => BlobPipelineOptions.DefaultApiVersion);

            var cmd = new Command(optName, optDescription)
            {
                fileOption,
                blockSize,
                writers,
                readers,
                apiVersion
            };

            foreach (var option in options)
            {
                cmd.AddOption(option);
            }

            return cmd;
        }
    }
}
