// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Logging;

namespace Tes.Runner.Logs;

public class ConsoleStreamLogPublisher(ILogger<ConsoleStreamLogPublisher> logger) : StreamLogReader(logger)
{
    public override async Task AppendStandardOutputAsync(string data)
    {
        await Console.Out.WriteAsync(data);
    }

    public override async Task AppendStandardErrAsync(string data)
    {
        await Console.Error.WriteAsync(data);
    }

    public override void OnComplete(Exception? err)
    {
        if (err != null)
        {
            Console.Error.WriteLine(err.ToString());
        }
    }
}
