// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.CommandLine;
using Tes.RunnerCLI.Commands;

return await StartUpAsync(args);

static async Task<int> StartUpAsync(string[] args)
{
    var rootCommand = CommandFactory.CreateExecutorCommand();
    CommandFactory.CreateUploadCommand(rootCommand);
    CommandFactory.CreateDownloadCommand(rootCommand);


    return await rootCommand.InvokeAsync(args);
}
