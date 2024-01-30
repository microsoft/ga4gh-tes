// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace TesApi.Web.Middleware
{
    public class RawRequestBodyLoggingMiddleware
    {
        private readonly RequestDelegate _next;
        private readonly ILogger<RawRequestBodyLoggingMiddleware> logger;

        public RawRequestBodyLoggingMiddleware(RequestDelegate next, ILogger<RawRequestBodyLoggingMiddleware> logger)
        {
            _next = next;
            this.logger = logger;
        }

        public async Task InvokeAsync(HttpContext context)
        {
            try
            {
                if (context.Request.Method == HttpMethods.Post && context.Request.Path.StartsWithSegments("/v1/tasks"))
                {
                    // Only store newly created TesTasks
                    context.Request.EnableBuffering(bufferThreshold: 100_000_000, bufferLimit: 50_000_000);
                    context.Request.Body.Position = 0;

                    using var reader = new StreamReader(context.Request.Body);
                    string json = await reader.ReadToEndAsync();
                    context.Request.Body.Position = 0;
                    string filePath = Path.Combine(Path.GetTempPath(), $"{DateTime.UtcNow:yyyyMMdd_HHmmss}.json");
                    await File.WriteAllTextAsync(filePath, json);
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "RawRequestBodyLoggingMiddleware");
            }

            _ = _next.Invoke(context);
        }
    }
}
