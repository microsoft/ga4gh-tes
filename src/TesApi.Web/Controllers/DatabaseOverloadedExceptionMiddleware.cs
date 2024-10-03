// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Tes.Repository;

namespace TesApi.Web.Controllers
{
    /// <summary>
    /// Handles any controllers that throws an exception of type DatabaseOverloadedException
    /// </summary>
    /// <param name="next">A function that can process an HTTP request.</param>
    public class DatabaseOverloadedExceptionMiddleware(RequestDelegate next)
    {
        private readonly RequestDelegate nextRequestDelegate = next;

        /// <summary>
        /// Default InvokeAsync implementation
        /// </summary>
        /// <param name="context">Encapsulates all HTTP-specific information about an individual HTTP request.</param>
        /// <returns></returns>
        public async Task InvokeAsync(HttpContext context)
        {
            try
            {
                await nextRequestDelegate(context);
            }
            catch (DatabaseOverloadedException ex)
            {
                context.Response.StatusCode = StatusCodes.Status429TooManyRequests;
                context.Response.ContentType = "text/plain";
                await context.Response.WriteAsync(ex.Message);
            }
        }
    }
}
