﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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
        /// <returns></returns>
        IBatchPool CreateNew();
    }
}
