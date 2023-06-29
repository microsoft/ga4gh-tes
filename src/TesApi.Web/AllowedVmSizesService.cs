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
    /// Service that periodically fetches the allowed vms list from storage, and updates the supported vms list.
    /// </summary>
    public class AllowedVmSizesService : BackgroundService, IAllowedVmSizesService
    {
        private readonly TimeSpan refreshInterval = TimeSpan.FromHours(24);
        private readonly ILogger logger;
        private readonly ConfigurationUtils configUtils;
        private List<string> allowedVmSizes;
        private Task firstTask;

        /// <summary>
        /// Service that periodically fetches the allowed vms list from storage, and updates the supported vms list.
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

        private async Task GetAllowedVmSizesImpl(CancellationToken cancellationToken)
        {
            try
            {
                logger.LogInformation("Executing allowed vm sizes config setup");
                allowedVmSizes = await configUtils.ProcessAllowedVmSizesConfigurationFileAsync(cancellationToken);
            }
            catch (Exception e)
            {
                logger.LogError(e, "Failed to execute allowed vm sizes config setup");
                throw;
            }
        }

        /// <inheritdoc/>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            firstTask = GetAllowedVmSizesImpl(stoppingToken);
            await firstTask;

            using PeriodicTimer timer = new(refreshInterval);

            try
            {
                while (await timer.WaitForNextTickAsync(stoppingToken))
                {
                    await GetAllowedVmSizesImpl(stoppingToken);
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
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <returns>List of allowed vms.</returns>
        public async Task<List<string>> GetAllowedVmSizes(CancellationToken cancellationToken)
        {
            if (allowedVmSizes == null)
            {
                while (firstTask is null)
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
                }
                await firstTask;
            }

            return allowedVmSizes;
        }
    }
}
