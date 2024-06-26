// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;

namespace TesApi.Web
{
    internal class AzureBatchVirtualMachineAvailabilityException : Exception
    {
        public AzureBatchVirtualMachineAvailabilityException()
        {
        }

        public AzureBatchVirtualMachineAvailabilityException(string message) : base(message)
        {
        }

        public AzureBatchVirtualMachineAvailabilityException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
