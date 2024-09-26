// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;

namespace Tes.Repository
{
    public class DatabaseOverloadedException(Exception exception)
        : Exception("The database is currently overloaded; consider scaling the database up or reduce the number of requests", innerException: exception)
    { }
}
