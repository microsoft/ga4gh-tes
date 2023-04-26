// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.CommandLine;
using Tes.Runner;
using Tes.Runner.Docker;
using Tes.Runner.Transfer;

await StartUpAsync(args);

static async Task StartUpAsync(string[] args)
{
    var fileOption = new Option<FileInfo?>(
        name: "--file",
        description: "The file with the task definition.");

    var dockerUriOption = new Option<Uri?>(
        name: "--docker-url",
        description: "local docker engine endpoint"
    );

    dockerUriOption.SetDefaultValue(new Uri("unix:///var/run/docker.sock"));

    var rootCommand = new RootCommand("Execute a TES Task on the Node");
    rootCommand.AddOption(fileOption);
    rootCommand.AddOption(dockerUriOption);

    rootCommand.SetHandler((file, uri) => ExecuteNodeTask(file!, uri!),
        fileOption, dockerUriOption);

    await rootCommand.InvokeAsync(args);
}

static void ExecuteNodeTask(FileInfo file, Uri dockerUri)
{
    const int MiB = 1024 * 1024;

    var blockSize = 50 * MiB;

    var options = new BlobPipelineOptions(blockSize, 5, 15, 15);

    Console.WriteLine("Executing Executor");


    var executor = new Executor(new DockerExecutor(dockerUri));

    var content = File.ReadAllText(file.FullName);

    var result = executor.ExecuteNodeTaskAsync(content, options).Result;

    var logs = result.ContainerResult.Logs.ReadOutputToEndAsync(CancellationToken.None).Result;

    Console.WriteLine(@$"
 Execution Result:
 {logs}
 Total Bytes Downloaded: {result.InputsLength}
 Total Bytes Uploaded: {result.OutputsLength}
");
}
