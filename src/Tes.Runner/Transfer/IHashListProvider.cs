// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Transfer;

public interface IHashListProvider
{
    string CalculateAndAddBlockHash(PipelineBuffer pipelineBuffer);
    string GetRootHash();
}
