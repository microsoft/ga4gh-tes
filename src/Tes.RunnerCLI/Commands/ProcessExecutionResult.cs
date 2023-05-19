namespace Tes.RunnerCLI.Commands;

public record ProcessExecutionResult(string StandardOutput, string StandardError, int ExitCode);
