// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Tes.Models;

namespace TesApi.Web
{
    /// <inheritdoc/>
    public interface ITesTaskAppender
    {
        /// <inheritdoc/>
        void Append(TesTask tesTask);
    }

    /// <inheritdoc/>
    public class TesTaskAppender : BackgroundService, ITesTaskAppender
    {
        private readonly ConcurrentQueue<TesTask> tesTaskJsonAppendQueue = new ConcurrentQueue<TesTask>();
        private readonly ILogger<TesTaskAppender> logger;
        private readonly IAzureProxy azureProxy;

        /// <inheritdoc/>
        public TesTaskAppender(ILogger<TesTaskAppender> logger, IAzureProxy azureProxy)
        {
            this.logger = logger;
            this.azureProxy = azureProxy;
        }

        /// <inheritdoc/>
        public void Append(TesTask tesTask)
        {
            tesTaskJsonAppendQueue.Enqueue(tesTask);
        }

        /// <inheritdoc/>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                while (!stoppingToken.IsCancellationRequested && tesTaskJsonAppendQueue.TryDequeue(out TesTask tesTask))
                {
                    var jsonl = JsonConvert.SerializeObject(tesTask) + "\n";

                    try
                    {
                        // TODO auth, retries, exception handling
                        await azureProxy.UploadBlockToTesTasksAppendBlobAsync(null, jsonl, stoppingToken);
                    }
                    catch (Exception exc)
                    {
                        logger.LogError(exc, exc.Message);
                    }
                }

                await Task.Delay(TimeSpan.FromSeconds(1));
            }
        }
    }
}
