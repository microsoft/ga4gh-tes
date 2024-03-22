// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tes.ApiClients
{
    public class DrsHubApiClient : HttpApiClient
    {
        private readonly Uri drsHubUrl;

        public DrsHubApiClient(Uri drsHubUrl ) {
            ArgumentNullException.ThrowIfNull( drsHubUrl );

            this.drsHubUrl = drsHubUrl;
        }
    }
}
