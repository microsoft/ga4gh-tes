// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TesApi.Web
{
    /// <summary>
    /// Hosted service that executes one-time set up tasks at start up.
    /// </summary>
    public class AllowedVmSizesService : BackgroundService, IAllowedVmSizesService
    {
        private readonly ILogger logger;
        private readonly ConfigurationUtils configUtils;
        private List<string> allowedVmSizes;
        private Task firstTask;

        /// <summary>
        /// Hosted service that executes one-time set-up tasks at start up.
        /// </summary>
        /// <param name="configUtils"></param>
        /// <param name="logger"></param>
        public AllowedVmSizesService(ConfigurationUtils configUtils, ILogger<AllowedVmSizesService> logger)
        {
            ArgumentNullException.ThrowIfNull(configUtils);
            ArgumentNullException.ThrowIfNull(logger);

            this.configUtils = configUtils;
            this.logger = logger;
        }

        /// <inheritdoc/>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var getAllowedVms = async () =>
            {
                using (logger.BeginScope("Executing Start Up tasks"))
                {
                    try
                    {
                        logger.LogInformation("Executing Configuration Utils Setup");
                        allowedVmSizes = await configUtils.ProcessAllowedVmSizesConfigurationFileAsync();
                    }
                    catch (Exception e)
                    {
                        logger.LogError(e, "Failed to execute start up tasks");
                        throw;
                    }
                }
            };
            firstTask = Task.Run(getAllowedVms);
            await firstTask;

            using PeriodicTimer timer = new(TimeSpan.FromHours(24));

            try
            {
                while (await timer.WaitForNextTickAsync(stoppingToken))
                {
                    await Task.Run(getAllowedVms);
                }
            }
            catch (OperationCanceledException)
            {
                logger.LogInformation("AllowedVmSizes Service is stopping.");
            }
        }

        /// <summary>
        /// Awaits start up and then return allowed vm sizes. 
        /// </summary>
        /// <returns></returns>
        public async Task<List<string>> GetAllowedVmSizes()
        {
            if (allowedVmSizes == null)
            {
                while (firstTask is null)
                {
                    await Task.Delay(TimeSpan.FromSeconds(10));
                }
                await firstTask;
            }

            return allowedVmSizes;
        }
    }
}
