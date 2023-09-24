using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Tes.Models;
using TesApi.Web;

namespace TesApi.Controllers
{
    /// <summary>
    /// API endpoints for TaskQueueItems.
    /// </summary>
    public class TaskQueueItemsApiController : ControllerBase
    {
        private readonly ILogger<TaskQueueItemsApiController> logger;
        private readonly ITaskQueueItemProcessor processor;

        /// <summary>
        /// Construct a TaskQueueItemsApiController.
        /// </summary>
        /// <param name="repository">The main TaskQueueItem database repository</param>
        /// <param name="logger">The logger instance</param>
        public TaskQueueItemsApiController(ILogger<TaskQueueItemsApiController> logger, ITaskQueueItemProcessor taskQueueItemProcessor, IRetryPolicyProvider retryPolicyProvider)
        {
            this.logger = logger;
            this.processor = taskQueueItemProcessor;
        }

        /// <summary>
        /// Acquire lock for a TaskQueueItem.
        /// </summary>
        /// <param name="poolId">The ID of the pool.</param>
        /// <param name="cancellationToken">A CancellationToken for controlling the lifetime of the asynchronous operation.</param>
        [HttpPost]
        [Route("/taskqueueitems/acquire-lock/{poolId}")]
        public virtual async Task<IActionResult> AcquireLock(string poolId, CancellationToken cancellationToken)
        {
            if (string.IsNullOrEmpty(poolId))
            {
                return BadRequest("PoolId is required.");
            }

            TaskQueueItemFull item;

            if (!processor.TryDequeue(poolId, out item))
            {
                return NoContent();
            }

            this.logger.LogInformation("Acquired lock for TaskQueueItem {TaskQueueItemId}, TesTask {TesTaskId}", item.Id, item.TesTask.Id);
            return Ok(item);
        }


        [HttpPatch]
        [Route("/taskqueueitems/{id}")]
        public virtual async Task<IActionResult> UpdateTaskQueueItem([FromRoute] string id, [FromBody] TaskQueueItemBasic item, CancellationToken cancellationToken)
        {
            if (id != item.Id)
            {
                return BadRequest("ID in the URL does not match ID in the request body.");
            }

            if (!processor.ContainsKey(id))
            {
                // Doesn't exist, or, a rogue node that had previously stopped sending a ping and lost its lock
                return NotFound();
            }

            await this.processor.UpdateTaskQueueItemAsync(item, cancellationToken);
            return Ok();
        }
    }
}
