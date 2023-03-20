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
        private readonly RequestDelegate nextRequestDelegate;

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="next"></param>
        public DatabaseOverloadedExceptionMiddleware(RequestDelegate next)
        {
            this.nextRequestDelegate = next;
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
