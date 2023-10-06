// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Docker;

public class ConsoleStreamLogReader : MultiplexedStreamLogReader
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
