// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.CommandLine;
using Tes.RunnerCLI.Commands;

await StartUpAsync(args);

static async Task StartUpAsync(string[] args)
{

    var rootCommand = new RootCommand();
    var execCommand = CommandFactory.CreateExecutorCommand();
    var uploadCommand = CommandFactory.CreateUploadCommand();
    var downloadCommand = CommandFactory.CreateDownloadCommand();

    rootCommand.AddCommand(downloadCommand);
    rootCommand.AddCommand(uploadCommand);
    rootCommand.AddCommand(execCommand);

    await rootCommand.InvokeAsync(args);
}
