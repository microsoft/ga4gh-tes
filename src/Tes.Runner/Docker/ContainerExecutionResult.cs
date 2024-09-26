// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Docker
{
    public record ContainerExecutionResult(string Id, string? Error, long ExitCode);
}
