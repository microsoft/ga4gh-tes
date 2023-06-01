// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.RunnerCLI.Commands;

public record ProcessExecutionResult(string StandardOutput, string StandardError, int ExitCode);
