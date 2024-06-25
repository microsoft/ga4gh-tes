// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;

namespace Tes.Repository
{
    public class RepositoryCollisionException<T> : Exception where T : RepositoryItem<T>
    {
        public System.Threading.Tasks.Task<T> Task { get; }

        public RepositoryCollisionException(System.Threading.Tasks.Task<T> task)
        {
            Task = task;
        }

        public RepositoryCollisionException(string message, System.Threading.Tasks.Task<T> task) : base(message)
        {
            Task = task;
        }

        public RepositoryCollisionException(string message, Exception innerException, System.Threading.Tasks.Task<T> task) : base(message, innerException)
        {
            Task = task;
        }
    }
}
