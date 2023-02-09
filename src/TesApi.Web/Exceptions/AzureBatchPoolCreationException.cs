// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Runtime.Serialization;

namespace TesApi.Web
{
    [Serializable]
    internal class AzureBatchPoolCreationException : Exception
    {
        public AzureBatchPoolCreationException()
        {
        }

        public AzureBatchPoolCreationException(string message) : base(message)
        {
        }

        public AzureBatchPoolCreationException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected AzureBatchPoolCreationException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
