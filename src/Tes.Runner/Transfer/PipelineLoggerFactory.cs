// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Logging;

namespace Tes.Runner.Transfer
{
    public static class PipelineLoggerFactory
    {
        private static readonly ILoggerFactory SLogFactory = LoggerFactory.Create(builder => builder.AddConsole());

        public static ILogger<T> Create<T>()
        {
            return SLogFactory.CreateLogger<T>();
        }
    }
}
