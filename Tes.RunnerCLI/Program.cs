// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.CommandLine;
using Tes.RunnerCLI.Commands;

await StartUpAsync(args);

static async Task StartUpAsync(string[] args)
{


    var rootCommand = CommandFactory.CreateExecutorCommand();
    CommandFactory.CreateUploadCommand(rootCommand);
    CommandFactory.CreateDownloadCommand(rootCommand);


    await rootCommand.InvokeAsync(args);
}
