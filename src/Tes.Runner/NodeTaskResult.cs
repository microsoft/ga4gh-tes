// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Runner.Docker;

namespace Tes.Runner;

public record NodeTaskResult(ContainerExecutionResult ContainerResult, long InputsLength, long OutputsLength);
