// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.Extensions.Logging;

namespace TesApi.Web.Controllers
{
    /// <summary>
    /// Global filter that handles <see cref="OperationCanceledException"/>.
    /// </summary>
    public class OperationCancelledExceptionFilter : ExceptionFilterAttribute
    {
        private readonly ILogger _logger;

        /// <summary>
        /// Constructor for <see cref="OperationCancelledExceptionFilter"/>
        /// </summary>
        /// <param name="logger"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public OperationCancelledExceptionFilter(ILogger<OperationCancelledExceptionFilter> logger)
            => _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        /// <inheritdoc/>
        public override void OnException(ExceptionContext context)
        {
            if (context.Exception is OperationCanceledException)
            {
                _logger.LogInformation(context.Exception, "Request was cancelled");
                // TODO: 503? If so, how to add "Retry-After" and ensure that caching-related headers are not enabling caching of this result?
                // Alternatively: 429? Is there a built-in IActionResult for that?
                // If we are overloaded we do want to signal readyness for a retry, but this may also signal the service being shutdown.
                context.Result = new StatusCodeResult((int)System.Net.HttpStatusCode.GatewayTimeout);
                context.ExceptionHandled = true;
            }
        }
    }
}
