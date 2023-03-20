namespace TesApi.Web
{
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Tes.Repository;

    /// <summary>
    /// Handles any controllers that throws an exception of type DatabaseOverloadedException
    /// </summary>
    public class DatabaseOverloadedExceptionMiddleware
    {
        private readonly RequestDelegate _next;

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="next"></param>
        public DatabaseOverloadedExceptionMiddleware(RequestDelegate next)
        {
            _next = next;
        }

        /// <summary>
        /// Default InvokeAsync implementation
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task InvokeAsync(HttpContext context)
        {
            try
            {
                await _next(context);
            }
            catch (DatabaseOverloadedException)
            {
                context.Response.StatusCode = StatusCodes.Status429TooManyRequests;
                context.Response.ContentType = "text/plain";
                await context.Response.WriteAsync("The database is currently overloaded; consider scaling the database up or reduce the number of requests");
            }
        }
    }
}
