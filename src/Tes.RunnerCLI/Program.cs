// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.CommandLine;
using Tes.RunnerCLI;
using Tes.RunnerCLI.Commands;

return await StartUpAsync(args);

static async Task<int> StartUpAsync(string[] args)
{
    var rootCommand = CommandFactory.CreateRootCommand();
    CommandFactory.CreateUploadCommand(rootCommand);
    CommandFactory.CreateExecutorCommand(rootCommand);
    CommandFactory.CreateDownloadCommand(rootCommand);

    try
    {
        return await rootCommand.InvokeAsync(args);
    }
    catch (Exception ex)
    {
        Console.WriteLine(ex.ToString());
        return (int)ProcessExitCode.UncategorizedError;
    }
}
