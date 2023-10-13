// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using Microsoft.Extensions.DependencyInjection;

namespace TesApi.Web
{
    // TODO: Consider moving this class's implementation to Startup
    /// <summary>
    /// Factory to create BatchPool instances.
    /// </summary>
    public sealed class BatchPoolFactory : IBatchPoolFactory
    {
        private readonly IServiceProvider _serviceProvider;

        /// <summary>
        /// Constructor for <see cref="BatchPoolFactory"/>.
        /// </summary>
        /// <param name="serviceProvider">A service object.</param>
        public BatchPoolFactory(IServiceProvider serviceProvider)
            => _serviceProvider = serviceProvider;

        /// <inheritdoc/>
        public IBatchPool CreateNew()
            => _serviceProvider.GetService<BatchPool>();
    }
}
