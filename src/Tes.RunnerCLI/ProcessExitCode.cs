// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.RunnerCLI
{
    public enum ProcessExitCode
    {
        Success = 0,
        UncategorizedError = 10,
        IdentityUnavailable = 30,


        /*
        consider going down from 88 towards 78 (not inclusive) for "general" codes

        -- section boundaries (based on nice even base-2 boundaries) --

            88

            96

            112

            120

        ----------
        reserved (these can be used as appropriate, but only when there's no need to disambiguate)
             ---
            long-standing exit status convention for normal termination, i.e. not by signal:

                Exit status 0: success
                Exit status 1: "failure", as defined by the program
                Exit status 2: command line usage error

             there are common conventions when a program starts another:

                126 if the other program was found but could not be run
                127 if the other program could not be found
                128 plus the signal number if the process terminated abnormally (note: POSIX only specifies "greater than 128" and that kill -l exitstatus can be used to find the signal's short name)

            Most binaries (especially shells) seem to use 1-63 based on disordered spot-checking of man pages

            64-78 sysexits (search for sysexits.h)
            126-128 conventions for subprocess handling
            129-255 exits due to "unhandled" signals
            numbers above 255 are not possible in Linux (and will be wrapped).
         */

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
