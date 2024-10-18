// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TesApi.Web
{
    /// <summary>
    /// A background service that montitors CloudPools in the batch system, orchestrates their lifecycle, and updates their state.
    /// This should only be used as a system-wide singleton service.  This class does not support scale-out on multiple machines,
    /// nor does it implement a leasing mechanism.  In the future, consider using the Lease Blob operation.
    /// </summary>
    public class BatchPoolService : BackgroundService
    {
        private readonly IBatchScheduler _batchScheduler;
        private readonly ILogger _logger;

        /// <summary>
        /// Interval between each call to <see cref="IBatchPool.ServicePoolAsync(CancellationToken)"/>.
        /// </summary>
        public static readonly TimeSpan RunInterval = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Default constructor
        /// </summary>
        /// <param name="batchScheduler"></param>
        /// <param name="logger"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public BatchPoolService(IBatchScheduler batchScheduler, ILogger<BatchPoolService> logger)
        {
            _batchScheduler = batchScheduler ?? throw new ArgumentNullException(nameof(batchScheduler));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <inheritdoc />
        public override Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Batch Pools starting...");
            return base.StartAsync(cancellationToken);
        }

        /// <inheritdoc />
        public override Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Batch Pools stopping...");
            return base.StopAsync(cancellationToken);
        }

        /// <inheritdoc />
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Batch Pools started.");
            _batchScheduler.LoadExistingPoolsAsync(stoppingToken).Wait(stoppingToken); // Delay starting Scheduler until this completes to finish initializing BatchScheduler.

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await ServiceBatchPools(stoppingToken);
                    await Task.Delay(RunInterval, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception exc)
                {
                    _logger.LogError(exc, @"{ExceptionMessage}", exc.Message);
                }
            }

            _logger.LogInformation("Batch Pools gracefully stopped.");
        }

        /// <summary>
        /// Retrieves all batch pools from the database and affords an opportunity to react to changes.
        /// </summary>
        /// <param name="cancellationToken">A System.Threading.CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        /// <returns></returns>
        private async ValueTask ServiceBatchPools(CancellationToken cancellationToken)
        {
            var pools = _batchScheduler.GetPools().ToList();

            if (0 == pools.Count)
            {
                return;
            }

            var startTime = DateTime.UtcNow;

            foreach (var pool in pools)
            {
                try
                {
                    await pool.ServicePoolAsync(cancellationToken);
                }
                catch (Exception exc)
                {
                    _logger.LogError(exc, "Batch pool {PoolId} threw an exception in ServiceBatchPools.", pool.PoolId);
                }
            }

            _logger.LogDebug(@"ServiceBatchPools for {PoolsCount} pools completed in {TotalSeconds} seconds.", pools.Count, DateTime.UtcNow.Subtract(startTime).TotalSeconds);
        }
    }
}
