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
    public class DoOnceAtStartUpService : BackgroundService
    {
        private readonly ILogger logger;
        private readonly ConfigurationUtils configUtils;

        /// <summary>
        /// Hosted service that executes one-time set-up tasks at start up.
        /// </summary>
        /// <param name="configUtils"></param>
        /// <param name="logger"></param>
        public DoOnceAtStartUpService(ConfigurationUtils configUtils, ILogger<DoOnceAtStartUpService> logger)
        {
            ArgumentNullException.ThrowIfNull(configUtils);
            ArgumentNullException.ThrowIfNull(logger);

            this.configUtils = configUtils;
            this.logger = logger;
        }

        /// <inheritdoc/>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using (logger.BeginScope("Executing Start Up tasks"))
            {
                try
                {
                    logger.LogInformation("Executing Configuration Utils Setup");
                    await configUtils.ProcessAllowedVmSizesConfigurationFileAsync(stoppingToken);
                }
                catch (Exception e)
                {
                    logger.LogError(e, "Failed to execute start up tasks");
                    throw;
                }
            }
        }
    }
}
