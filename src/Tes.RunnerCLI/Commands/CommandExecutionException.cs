// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.RunnerCLI.Commands
{
    public class CommandExecutionException : Exception
    {
        public int ExitCode { get; private set; }

        public CommandExecutionException(int exitCode, string message, Exception innerException) : base(message, innerException)
        {
            ExitCode = exitCode;
        }
        public CommandExecutionException(int exitCode, string message) : base(message)
        {
            ExitCode = exitCode;
        }
    }
}
