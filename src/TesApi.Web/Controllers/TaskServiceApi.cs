﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

/*
 * Task Execution Service
 *
 * No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)
 *
 * OpenAPI spec version: 0.3.0
 * 
 * Generated by: https://openapi-generator.tech
 */

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Swashbuckle.AspNetCore.Annotations;
using Tes.Models;
using Tes.Repository;
using TesApi.Attributes;
using TesApi.Web;

namespace TesApi.Controllers
{
    /// <summary>
    /// API endpoints for <see cref="TesTask"/>s.
    /// </summary>
    public class TaskServiceApiController : ControllerBase
    {
        //private const string rootExecutionPath = "/tes-internal";
        private readonly IRepository<TesTask> repository;
        private readonly ILogger<TaskServiceApiController> logger;
        private readonly IAzureProxy azureProxy;

        private static readonly Dictionary<TesView, JsonSerializerSettings> TesJsonSerializerSettings = new()
        {
            { TesView.MINIMAL, new JsonSerializerSettings{ ContractResolver = MinimalTesTaskContractResolver.Instance } },
            { TesView.BASIC, new JsonSerializerSettings{ ContractResolver = BasicTesTaskContractResolver.Instance } },
            { TesView.FULL, new JsonSerializerSettings{ ContractResolver = FullTesTaskContractResolver.Instance } }
        };

        /// <summary>
        /// Contruct a <see cref="TaskServiceApiController"/>
        /// </summary>
        /// <param name="repository">The main <see cref="TesTask"/> database repository</param>
        /// <param name="logger">The logger instance</param>
        /// <param name="azureProxy">The Azure Proxy instance</param>
        public TaskServiceApiController(IRepository<TesTask> repository, ILogger<TaskServiceApiController> logger, IAzureProxy azureProxy)
        {
            this.repository = repository;
            this.logger = logger;
            this.azureProxy = azureProxy;
        }

        /// <summary>
        /// Cancel a task
        /// </summary>
        /// <param name="id">The id of the <see cref="TesTask"/> to cancel</param>
        /// <response code="200"></response>
        [HttpPost]
        [Route("/v1/tasks/{id}:cancel")]
        [ValidateModelState]
        [SwaggerOperation("CancelTask")]
        [SwaggerResponse(statusCode: 200, type: typeof(object), description: "")]
        public virtual async Task<IActionResult> CancelTask([FromRoute][Required] string id)
        {
            TesTask tesTask = null;

            if (await repository.TryGetItemAsync(id, item => tesTask = item))
            {
                if (tesTask.State == TesState.COMPLETEEnum ||
                    tesTask.State == TesState.EXECUTORERROREnum ||
                    tesTask.State == TesState.SYSTEMERROREnum)
                {
                    logger.LogInformation($"Task {id} cannot be canceled because it is in {tesTask.State} state.");
                }
                else if (tesTask.State != TesState.CANCELEDEnum)
                {
                    logger.LogInformation("Canceling task");
                    tesTask.IsCancelRequested = true;
                    tesTask.State = TesState.CANCELEDEnum;
                    await repository.UpdateItemAsync(tesTask);
                }
            }
            else
            {
                return NotFound($"The task with id {id} does not exist.");
            }


            return StatusCode(200, new object());
        }

        /// <summary>
        /// Create a new task                               
        /// </summary>
        /// <param name="tesTask">The <see cref="TesTask"/> to add to the repository</param>
        /// <response code="200"></response>
        [HttpPost]
        [Route("/v1/tasks")]
        [ValidateModelState]
        [SwaggerOperation("CreateTask")]
        [SwaggerResponse(statusCode: 200, type: typeof(TesCreateTaskResponse), description: "")]
        public virtual async Task<IActionResult> CreateTaskAsync([FromBody] TesTask tesTask)
        {
            if (!string.IsNullOrWhiteSpace(tesTask.Id))
            {
                return BadRequest("Id should not be included by the client in the request; the server is responsible for generating a unique Id.");
            }

            if (string.IsNullOrWhiteSpace(tesTask.Executors?.FirstOrDefault()?.Image))
            {
                return BadRequest("Docker container image name is required.");
            }

            tesTask.State = TesState.QUEUEDEnum;
            tesTask.CreationTime = DateTimeOffset.UtcNow;

            // example: /tes-internal/test/daf1a044-d741-4db9-8eb5-d6fd0519b1f1/call-hello/execution/script
            tesTask.WorkflowId = tesTask
                ?.Inputs
                ?.FirstOrDefault(i => i?.Name?.Equals("commandScript", StringComparison.OrdinalIgnoreCase) == true)
                ?.Path
                ?.Split('/', StringSplitOptions.RemoveEmptyEntries)
                ?.Skip(2)
                ?.FirstOrDefault();

            // Prefix the TES task id with first eight characters of root Cromwell job id to facilitate easier debugging
            var tesTaskIdPrefix = tesTask.WorkflowId is not null && Guid.TryParse(tesTask.WorkflowId, out _) ? $"{tesTask.WorkflowId.Substring(0, 8)}_" : string.Empty;
            tesTask.Id = $"{tesTaskIdPrefix}{Guid.NewGuid():N}";

            // For CWL workflows, if disk size is not specified in TES object (always), try to retrieve it from the corresponding workflow stored by Cromwell in /cromwell-tmp directory
            // Also allow for TES-style "memory" and "cpu" hints in CWL.
            if (tesTask.Name is not null
                && tesTask.Inputs?.Any(i => i.Path.Contains(".cwl/")) == true
                && tesTask.WorkflowId is not null
                && azureProxy.TryReadCwlFile(tesTask.WorkflowId, out var cwlContent)
                && CwlDocument.TryCreate(cwlContent, out var cwlDocument))
            {
                tesTask.Resources ??= new TesResources();
                tesTask.Resources.DiskGb = tesTask.Resources.DiskGb ?? cwlDocument.DiskGb;
                tesTask.Resources.CpuCores = tesTask.Resources.CpuCores ?? cwlDocument.Cpu;
                tesTask.Resources.RamGb = tesTask.Resources.RamGb ?? cwlDocument.MemoryGb;

                // Preemptible is not passed on from CWL workflows to Cromwell, so Cromwell sends the default (TRUE) to TES, 
                // instead of NULL like the other values above.
                // If CWL document has it specified, override the value sent by Cromwell
                tesTask.Resources.Preemptible = cwlDocument.Preemptible ?? tesTask.Resources.Preemptible;
            }

            if (tesTask?.Resources?.BackendParameters is not null)
            {
                var keys = tesTask.Resources.BackendParameters.Keys.Select(k => k).ToList();

                if (keys.Count > 1 && keys.Select(k => k?.ToLowerInvariant()).Distinct().Count() != keys.Count)
                {
                    return BadRequest("Duplicate backend_parameters were specified");
                }

                // Force all keys to be lowercase
                tesTask.Resources.BackendParameters = new Dictionary<string, string>(
                    tesTask.Resources.BackendParameters.Select(k => new KeyValuePair<string, string>(k.Key?.ToLowerInvariant(), k.Value)));

                keys = tesTask.Resources.BackendParameters.Keys.Select(k => k).ToList();

                // Backends shall log system warnings if a key is passed that is unsupported.
                var unsupportedKeys = keys.Except(Enum.GetNames(typeof(TesResources.SupportedBackendParameters))).ToList();

                if (unsupportedKeys.Count > 0)
                {
                    logger.LogWarning($"Unsupported keys were passed to TesResources.backend_parameters: {string.Join(",", unsupportedKeys)}");
                }

                // If backend_parameters_strict equals true, backends should fail the task if any key / values are unsupported
                if (tesTask.Resources?.BackendParametersStrict == true
                    && unsupportedKeys.Count > 0)
                {
                    return BadRequest($"backend_parameters_strict is set to true and unsupported backend_parameters were specified: {string.Join(",", unsupportedKeys)}");
                }

                // Backends shall not store or return unsupported keys if included in a task.
                foreach (var key in unsupportedKeys)
                {
                    tesTask.Resources.BackendParameters.Remove(key);
                }
            }

            logger.LogDebug($"Creating task with id {tesTask.Id} state {tesTask.State}");
            await repository.CreateItemAsync(tesTask);
            return StatusCode(200, new TesCreateTaskResponse { Id = tesTask.Id });
        }

        /// <summary>
        /// GetServiceInfo provides information about the service, such as storage details, resource availability, and  other documentation.
        /// </summary>
        /// <response code="200"></response>
        [HttpGet]
        [Route("/v1/service-info")]
        [ValidateModelState]
        [SwaggerOperation("GetServiceInfo")]
        [SwaggerResponse(statusCode: 200, type: typeof(TesServiceInfo), description: "")]
        public virtual IActionResult GetServiceInfo()
        {
            var serviceInfo = new TesServiceInfo
            {
                Name = "GA4GH Task Execution Service",
                Doc = string.Empty,
                Storage = new List<string>(),
                TesResourcesSupportedBackendParameters = Enum.GetNames(typeof(TesResources.SupportedBackendParameters)).ToList()
            };

            logger.LogInformation($"Name: {serviceInfo.Name} Doc: {serviceInfo.Doc} Storage: {serviceInfo.Storage} TesResourcesSupportedBackendParameters: {string.Join(",", serviceInfo.TesResourcesSupportedBackendParameters)}");
            return StatusCode(200, serviceInfo);
        }

        /// <summary>
        /// Get a task. TaskView is requested as such: \&quot;v1/tasks/{id}?view&#x3D;FULL\&quot;
        /// </summary>
        /// <param name="id">The id of the <see cref="TesTask"/> to get</param>
        /// <param name="view">OPTIONAL. Affects the fields included in the returned Task messages. See TaskView below.   - MINIMAL: Task message will include ONLY the fields:   Task.Id   Task.State  - BASIC: Task message will include all fields EXCEPT:   Task.ExecutorLog.stdout   Task.ExecutorLog.stderr   Input.content   TaskLog.system_logs  - FULL: Task message includes all fields.</param>
        /// <response code="200"></response>
        [HttpGet]
        [Route("/v1/tasks/{id}")]
        [ValidateModelState]
        [SwaggerOperation("GetTask")]
        [SwaggerResponse(statusCode: 200, type: typeof(TesTask), description: "")]
        public virtual async Task<IActionResult> GetTaskAsync([FromRoute][Required] string id, [FromQuery] string view)
        {
            TesTask tesTask = null;
            var itemFound = await repository.TryGetItemAsync(id, item => tesTask = item);

            return itemFound ? TesJsonResult(tesTask, view) : NotFound($"The task with id {id} does not exist.");
        }

        /// <summary>
        /// List tasks. TaskView is requested as such: \&quot;v1/tasks?view&#x3D;BASIC\&quot;
        /// </summary>
        /// <param name="namePrefix">OPTIONAL. Filter the list to include tasks where the name matches this prefix. If unspecified, no task name filtering is done.</param>
        /// <param name="pageSize">OPTIONAL. Number of tasks to return in one page. Must be less than 2048. Defaults to 256.</param>
        /// <param name="pageToken">OPTIONAL. Page token is used to retrieve the next page of results. If unspecified, returns the first page of results. See ListTasksResponse.next_page_token</param>
        /// <param name="view">OPTIONAL. Affects the fields included in the returned Task messages. See TaskView below.   - MINIMAL: Task message will include ONLY the fields:   Task.Id   Task.State  - BASIC: Task message will include all fields EXCEPT:   Task.ExecutorLog.stdout   Task.ExecutorLog.stderr   Input.content   TaskLog.system_logs  - FULL: Task message includes all fields.</param>
        /// <response code="200"></response>
        [HttpGet]
        [Route("/v1/tasks")]
        [ValidateModelState]
        [SwaggerOperation("ListTasks")]
        [SwaggerResponse(statusCode: 200, type: typeof(TesListTasksResponse), description: "")]
        public virtual async Task<IActionResult> ListTasks([FromQuery] string namePrefix, [FromQuery] long? pageSize, [FromQuery] string pageToken, [FromQuery] string view)
        {
            var decodedPageToken =
                pageToken is not null ? Encoding.UTF8.GetString(Base64UrlTextEncoder.Decode(pageToken)) : null;

            if (pageSize < 1 || pageSize > 2047)
            {
                logger.LogError($"pageSize invalid {pageSize}");
                return BadRequest("If provided, pageSize must be greater than 0 and less than 2048. Defaults to 256.");
            }

            (var nextPageToken, var tasks) = await repository.GetItemsAsync(
                t => string.IsNullOrWhiteSpace(namePrefix) || t.Name.StartsWith(namePrefix),
                pageSize.HasValue ? (int)pageSize : 256,
                decodedPageToken);

            var encodedNextPageToken = nextPageToken is not null ? Base64UrlTextEncoder.Encode(Encoding.UTF8.GetBytes(nextPageToken)) : null;
            var response = new TesListTasksResponse { Tasks = tasks.ToList(), NextPageToken = encodedNextPageToken };

            return TesJsonResult(response, view);
        }

        private IActionResult TesJsonResult(object value, string view)
        {
            TesView viewEnum;

            try
            {
                viewEnum = string.IsNullOrEmpty(view) ? TesView.MINIMAL : Enum.Parse<TesView>(view, true);
            }
            catch
            {
                logger.LogError($"Invalid view parameter value. If provided, it must be one of: {string.Join(", ", Enum.GetNames(typeof(TesView)))}");
                return BadRequest($"Invalid view parameter value. If provided, it must be one of: {string.Join(", ", Enum.GetNames(typeof(TesView)))}");
            }

            var jsonResult = new JsonResult(value, TesJsonSerializerSettings[viewEnum]) { StatusCode = 200 };

            return jsonResult;
        }

        private enum TesView
        {
            MINIMAL,
            BASIC,
            FULL
        }
    }
}
