// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Core;

namespace Tes.ApiClients
{
    public class DrsHubApiClient : HttpApiClient
    {
        private readonly Uri drsHubUrl;

        public DrsHubApiClient(Uri drsHubUrl ) {
            ArgumentNullException.ThrowIfNull( drsHubUrl );

            this.drsHubUrl = drsHubUrl;
        }

        private HttpRequestMessage CreateResolveDrsRequest() {

            var requestUri = new Uri(drsHubUrl, new Uri("/api/v4/drs/resolve"));

            var request = new HttpRequestMessage();
            

            request.Method = RequestMethod.Post;
            var uri = new RawRequestUriBuilder();
            uri.Reset(_endpoint);
            uri.AppendPath("/api/v4/drs/resolve", false);
        }
    }
}
