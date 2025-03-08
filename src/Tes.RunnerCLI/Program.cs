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
    CommandFactory.CreateStartTaskCommand(rootCommand);

    try
    {
        return await Return(await rootCommand.InvokeAsync(args));
    }
    catch (Exception ex)
    {
        Console.WriteLine(ex.ToString());
        return await Return((int)ProcessExitCode.UncategorizedError);
    }

    static async ValueTask<int> Return(int exitCode)
    {
        var delayOnFailureStr = Environment.GetEnvironmentVariable("DEBUG_DELAY");

        if (exitCode != 0 && TimeSpan.TryParseExact(delayOnFailureStr, "c", System.Globalization.CultureInfo.InvariantCulture, out var delayOnFailure))
        {
            Console.WriteLine($"Delaying for {delayOnFailure} due to DEBUG_DELAY environment variable.");
            await Task.Delay(delayOnFailure);
        }

        return exitCode;
    }
}
