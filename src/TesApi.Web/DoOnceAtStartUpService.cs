// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TesApi.Web
{
    /// <summary>
    /// Hosted service that executes one-time set up tasks at start up.
    /// </summary>
    public class DoOnceAtStartUpService : IHostedService
    {
        private readonly ILogger logger;
        private readonly IServiceProvider serviceProvider;
        /// <summary>
        /// Hosted service that executes one-time set-up tasks at start up.
        /// </summary>
        /// <param name="serviceProvider"></param>
        /// <param name="logger"></param>
        public DoOnceAtStartUpService(IServiceProvider serviceProvider, ILogger<DoOnceAtStartUpService> logger)
        {
            ArgumentNullException.ThrowIfNull(serviceProvider);
            ArgumentNullException.ThrowIfNull(logger);

            this.serviceProvider = serviceProvider;
            this.logger = logger;
        }

        /// <summary>
        /// Executes start up tasks
        /// </summary>
        /// <param name="cancellationToken"></param>
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            using (logger.BeginScope("Executing Start Up tasks"))
            {
                try
                {
                    logger.LogInformation("Executing Configuration Utils Setup");
                    await ExecuteConfigurationUtilsSetupAsync();
                }
                catch (Exception e)
                {
                    logger.LogError(e, "Failed to execute start up tasks");
                    throw;
                }
            }
        }

        private async Task ExecuteConfigurationUtilsSetupAsync()
        {
            if (serviceProvider.GetService(typeof(ConfigurationUtils)) is not ConfigurationUtils configUtils)
            {
                throw new InvalidOperationException(
                    "Configuration utils instance can't be resolved from the app context");
            }

            await configUtils.ProcessAllowedVmSizesConfigurationFileAsync();
        }

        /// <summary>
        /// Stops service.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task StopAsync(CancellationToken cancellationToken)
        {
            logger.LogInformation("Service stopped");
            return Task.CompletedTask;
        }
    }
}
