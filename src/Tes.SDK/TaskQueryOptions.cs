// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Tes.Models;

namespace Tes.SDK
{
    public record TaskQueryOptions(
        TesView View = TesView.MINIMAL,
        Dictionary<string, string>? Tags = null,
        TesState? State = null,
        string? NamePrefix = null
    );
}
