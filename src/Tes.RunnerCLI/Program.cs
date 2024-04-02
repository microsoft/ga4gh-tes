// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.CommandLine;
using Tes.RunnerCLI.Commands;

return await StartUpAsync(args);

static async Task<int> StartUpAsync(string[] args)
{
    var rootCommand = CommandFactory.Instance.CreateRootCommand();
    CommandFactory.Instance.CreateUploadCommand(rootCommand);
    CommandFactory.Instance.CreateExecutorCommand(rootCommand);
    CommandFactory.Instance.CreateDownloadCommand(rootCommand);

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
