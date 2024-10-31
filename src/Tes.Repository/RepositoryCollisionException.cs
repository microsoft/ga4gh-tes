// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;

namespace Tes.Repository
{
    public class RepositoryCollisionException<T> : Exception where T : RepositoryItem<T>
    {
        public T RepositoryItem { get; }

        public RepositoryCollisionException(T repositoryItem)
        {
            RepositoryItem = repositoryItem;
        }

        public RepositoryCollisionException(string message, T repositoryItem) : base(message)
        {
            RepositoryItem = repositoryItem;
        }

        public RepositoryCollisionException(string message, Exception innerException, T repositoryItem) : base(message, innerException)
        {
            RepositoryItem = repositoryItem;
        }
    }
}
