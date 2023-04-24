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
                context.ExceptionHandled = true;
                context.Result = new StatusCodeResult(504); // TODO: 503?
            }
        }
    }
}
