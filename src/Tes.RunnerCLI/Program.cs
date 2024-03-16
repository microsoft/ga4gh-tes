// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.CommandLine;
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
    catch (CommandExecutionException ex)
    {
        if (ex.InnerException is not null)
        {
            Console.WriteLine(ex.InnerException.Message);
        }

        return ex.ExitCode;
    }
    catch (Exception ex)
    {
        Console.WriteLine(ex.ToString());
        return (int)ProcessExitCode.UncategorizedError;
    }
}
