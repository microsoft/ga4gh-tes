// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Azure.Batch;

namespace TesApi.Web
{
    /// <summary>
    /// Factory to create BatchPool instances.
    /// </summary>
    public interface IBatchPoolFactory
    {
        /// <summary>
        /// Creates <see cref="BatchPool"/> instances.
        /// </summary>
        /// <param name="poolId"></param>
        /// <remarks>Creates a BatchPool object to delete any pool with a matching Id that may have been created. Used when timeout errors happen during pool cration.</remarks>
        /// <returns></returns>
        IBatchPool CreateNew(string poolId);

        /// <summary>
        /// Creates <see cref="BatchPool"/> instances.
        /// </summary>
        /// <param name="poolInformation"></param>
        /// <remarks>Creates a BatchPool object to manage a newly created batch pool.</remarks>
        /// <returns></returns>
        IBatchPool CreateNew(PoolInformation poolInformation);

        /// <summary>
        /// Retrieves <see cref="BatchPool"/> instances.
        /// </summary>
        /// <param name="pool"></param>
        /// <remarks>Creates a BatchPool object to manange an existing batch pool.</remarks>
        /// <returns></returns>
        IBatchPool Retrieve(CloudPool pool);
    }
}
