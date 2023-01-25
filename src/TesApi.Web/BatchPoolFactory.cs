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
