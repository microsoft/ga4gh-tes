// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using Microsoft.Azure.Batch;
using Microsoft.Extensions.DependencyInjection;

namespace TesApi.Web
{
    /// <summary>
    /// Factory to create BatchPool instances.
    /// </summary>
    public sealed class BatchPoolFactory : IBatchPoolFactory
    {
        // Each factory method should correspond to a separate public constructor of any class implementing the
        // <see cref="IBatchPool"/> interface. It should only provide the parameters needed for the specific instance of
        // that class. No parameters that can reasonably be provided by dependency injection should be included in any of
        // these methods.

        private readonly IServiceProvider _serviceProvider;
        private readonly ObjectFactory _batchPoolAltCreator;
        private readonly ObjectFactory _batchPoolCreator;
        private readonly ObjectFactory _batchPoolRequester;

        /// <summary>
        /// Constructor for <see cref="BatchPoolFactory"/>.
        /// </summary>
        /// <param name="serviceProvider">A service object.</param>
        public BatchPoolFactory(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
            _batchPoolAltCreator = BatchPoolAltCreatorFactory();
            _batchPoolCreator = BatchPoolCreatorFactory();
            _batchPoolRequester = BatchPoolRequesterFactory();
        }

        /// <inheritdoc/>
        public IBatchPool CreateNew(string poolId)
            => (IBatchPool)_batchPoolAltCreator(_serviceProvider, new object[] { poolId });

        private static ObjectFactory BatchPoolAltCreatorFactory()
            => ActivatorUtilities.CreateFactory(typeof(BatchPool), new Type[] { typeof(string) });

        /// <inheritdoc/>
        public IBatchPool CreateNew(PoolInformation poolInformation)
            => (IBatchPool)_batchPoolCreator(_serviceProvider, new object[] { poolInformation });

        private static ObjectFactory BatchPoolCreatorFactory()
            => ActivatorUtilities.CreateFactory(typeof(BatchPool), new Type[] { typeof(PoolInformation) });

        /// <inheritdoc/>
        public IBatchPool Retrieve(CloudPool pool)
            => (IBatchPool)_batchPoolRequester(_serviceProvider, new object[] { pool });

        private static ObjectFactory BatchPoolRequesterFactory()
            => ActivatorUtilities.CreateFactory(typeof(BatchPool), new Type[] { typeof(CloudPool) });
    }
}
