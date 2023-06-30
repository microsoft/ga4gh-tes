// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TesApi.Tests.Integration
{
    internal class TestTerraEnvInfo
    {
        private const string LzStorageAccountNameEnvVarName = "TERRA_LZ_STORAGE_ACCOUNT";
        private const string WorkspaceContainerNameEnvVarName = "TERRA_WS_STORAGE_CONTAINER";
        private const string WsmApiHostEnvVarName = "TERRA_WSM_API_HOST";

        private readonly string lzStorageAccountName;
        private readonly string workspaceContainerName;
        private readonly string wsmApiHost;

        public string LzStorageAccountName => lzStorageAccountName;
        public string WorkspaceContainerName => workspaceContainerName;
        public string WsmApiHost => wsmApiHost;

        public TestTerraEnvInfo()
        {
            lzStorageAccountName = Environment.GetEnvironmentVariable(LzStorageAccountNameEnvVarName);
            workspaceContainerName = Environment.GetEnvironmentVariable(WorkspaceContainerNameEnvVarName);
            wsmApiHost = Environment.GetEnvironmentVariable(WsmApiHostEnvVarName);
            
            if (string.IsNullOrEmpty(lzStorageAccountName))
            {
                throw new InvalidOperationException(
                    $"The environment variable {LzStorageAccountNameEnvVarName} is not set");
            }

            if (string.IsNullOrEmpty(workspaceContainerName))
            {
                throw new InvalidOperationException(
                    $"The environment variable {WorkspaceContainerNameEnvVarName} is not set");
            }

            if (string.IsNullOrEmpty(wsmApiHost))
            {
                throw new InvalidOperationException(
                    $"The environment variable {WsmApiHostEnvVarName} is not set");
            }
        }
    }
}
