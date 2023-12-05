// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.RunnerCLI.Commands
{
    public enum ProcessExitCode
    {
        Success = 0,
        UncategorizedError = 1,
        // RunnerDownloadFailure = 10, // TODO reserved for Bash script
        IdentityUnavailable = 30,

        // TODO Implement all process exit codes
        //AzureStorageNotResponding = 80,
        //AzureStorageNotAuthorizedManagedIdentityCredential = 81,
        //AzureStorageNotAuthorizedDefaultCredential = 82,
        //AzureStorageNotAuthorizedInvalidSASToken = 83,
        //AzureStorageNotAuthorizedSASTokenExpired = 84,
        //TerraAPINotResponding = 90,
        //TerraAPINotAuthorizedManagedIdentityCredential = 91,
        //TerraAPINotAuthorizedDefaultCredential = 92,
        //TerraAPIBadRequest = 93
    }
}
