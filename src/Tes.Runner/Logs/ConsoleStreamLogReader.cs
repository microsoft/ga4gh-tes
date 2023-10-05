// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Logs;

public class ConsoleStreamLogReader : StreamLogReader
{
    public ConsoleStreamLogReader()
    {

    }

    public override Task AppendStandardOutputAsync(string data)
    {
        Console.Write(data);
        return Task.CompletedTask;
    }

    public override Task AppendStandardErrAsync(string data)
    {
        Console.Write(data);
        return Task.CompletedTask;
    }

    public override void OnComplete(Exception? err)
    {
        if (err != null)
        {
            Console.Write(err.ToString());
        }
    }
}
