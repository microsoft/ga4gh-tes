// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;
using System.Threading.Tasks;

namespace TesApi.Web
{
    /// <summary>
    /// Interface to get allowed vm sizes for TES.
    /// </summary>
    public interface IAllowedVmSizesService
    {
        /// <summary>
        /// Gets allowed vm sizes.
        /// </summary>
        /// <returns>A list of allowed vm sizes.</returns>
        Task<List<string>> GetAllowedVmSizes();
    }
}
