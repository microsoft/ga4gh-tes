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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Swashbuckle.AspNetCore.Annotations;
using Tes.Models;
using Tes.Repository;
using Tes.TaskSubmitters;
using TesApi.Attributes;
using TesApi.Web;

namespace TesApi.Controllers
{
    /// <summary>
    /// API endpoints for <see cref="TesTask"/>s.
    /// </summary>
    /// <remarks>
    /// Construct a <see cref="TaskServiceApiController"/>
    /// </remarks>
    /// <param name="repository">The main <see cref="TesTask"/> database repository</param>
    /// <param name="taskScheduler">The task scheduler</param>
    /// <param name="serviceInfo">The GA4GH TES service information</param>
    /// <param name="logger">The logger instance</param>
    public class TaskServiceApiController(IRepository<TesTask> repository, ITaskScheduler taskScheduler, TesServiceInfo serviceInfo, ILogger<TaskServiceApiController> logger)
        : ControllerBase
    {
        //private const string rootExecutionPath = "/cromwell-executions";
        private readonly IRepository<TesTask> repository = repository;
        private readonly ITaskScheduler taskScheduler = taskScheduler;
        private readonly ILogger<TaskServiceApiController> logger = logger;
        private readonly TesServiceInfo serviceInfo = serviceInfo;

        private static readonly Dictionary<TesView, JsonSerializerSettings> TesJsonSerializerSettings = new()
        {
            { TesView.MINIMAL, new JsonSerializerSettings{ ContractResolver = MinimalTesTaskContractResolver.Instance } },
            { TesView.BASIC, new JsonSerializerSettings{ ContractResolver = BasicTesTaskContractResolver.Instance } },
            { TesView.FULL, new JsonSerializerSettings{ ContractResolver = FullTesTaskContractResolver.Instance } }
        };

        /// <summary>
        /// Cancel a task
        /// </summary>
        /// <param name="id">The id of the <see cref="TesTask"/> to cancel</param>
        /// <param name="cancellationToken">A<see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <response code="200"></response>
        [HttpPost]
        [Route("/v1/tasks/{id}:cancel")]
        [ValidateModelState]
        [SwaggerOperation("CancelTask", "Cancel a task based on providing an exact task ID.")]
        [SwaggerResponse(statusCode: 200, type: typeof(object), description: "")]
        public virtual async Task<IActionResult> CancelTaskAsync([FromRoute][Required] string id, CancellationToken cancellationToken)
        {
            if (!TesTask.IsValidId(id))
            {
                return BadRequest("Invalid ID");
            }

            TesTask tesTask = null;

            if (await repository.TryGetItemAsync(id, cancellationToken, item => tesTask = item))
            {
                if (tesTask.State == TesState.COMPLETE ||
                    tesTask.State == TesState.EXECUTOR_ERROR ||
                    tesTask.State == TesState.SYSTEM_ERROR ||
                    tesTask.State == TesState.PREEMPTED ||
                    tesTask.State == TesState.CANCELING)
                {
                    logger.LogInformation("Task {TesTask} cannot be canceled because it is in {TesTaskState} state.", id, tesTask.State);
                }
                else if (tesTask.State != TesState.CANCELED)
                {
                    logger.LogInformation("Canceling task");
                    tesTask.State = TesState.CANCELING;

                    try
                    {
                        await repository.UpdateItemAsync(tesTask, cancellationToken);
                    }
                    catch (RepositoryCollisionException<TesTask> exc)
                    {
                        logger.LogError(exc, "RepositoryCollisionException in CancelTask for {TesTask}", id);
                        return Conflict(new { message = "The task could not be updated due to a conflict with the current state; please retry." });
                    }
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
        /// <param name="cancellationToken">A<see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <response code="200"></response>
        [HttpPost]
        [Route("/v1/tasks")]
        [ValidateModelState]
        [SwaggerOperation("CreateTask", "Create a new task. The user provides a Task document, which the server uses as a basis and adds additional fields.")]
        [SwaggerResponse(statusCode: 200, type: typeof(TesCreateTaskResponse), description: "")]
        public virtual async Task<IActionResult> CreateTaskAsync([FromBody] TesTask tesTask, CancellationToken cancellationToken)
        {
            if (!string.IsNullOrWhiteSpace(tesTask.Id))
            {
                return BadRequest("Id should not be included by the client in the request; the server is responsible for generating a unique Id.");
            }

            if (!(tesTask.Executors ?? []).Any())
            {
                return BadRequest("At least one executor is required.");
            }

            if ((tesTask.Executors ?? []).Select(executor => executor.Image).Any(string.IsNullOrWhiteSpace))
            {
                return BadRequest("Docker container image name is required.");
            }

            foreach (var executor in (tesTask.Executors ?? []))
            {
                if (string.IsNullOrWhiteSpace(executor.Image))
                {
                    return BadRequest("Docker container image name is required.");
                }

                if (!(executor.Command ?? []).Any())
                {
                    return BadRequest("Executor command is required.");
                }

                if (executor.Stdin is not null && !executor.Stdin.StartsWith('/'))
                {
                    return BadRequest("Standard in must be an absolute path in the container.");
                }

                if (executor.Stdout is not null && !executor.Stdout.StartsWith('/'))
                {
                    return BadRequest("Standard out must be an absolute path in the container.");
                }

                if (executor.Stderr is not null && !executor.Stderr.StartsWith('/'))
                {
                    return BadRequest("Standard error must be an absolute path in the container.");
                }
            }

            foreach (var input in tesTask.Inputs ?? [])
            {
                if (string.IsNullOrWhiteSpace(input.Path) || !input.Path.StartsWith('/'))
                {
                    return BadRequest("Input paths in the container must be absolute paths.");
                }

                if (input.Url?.StartsWith("file://") ?? false)
                {
                    return BadRequest("Input URLs to the local file system are not supported.");
                }

                if (input.Url is not null && input.Content is not null)
                {
                    return BadRequest("Input URL and Content cannot be both set");
                }

                if (input.Url is null && input.Content is null)
                {
                    return BadRequest("One of Input URL or Content must be set");
                }

                if (input.Content is not null && input.Type == TesFileType.DIRECTORY)
                {
                    return BadRequest("Content inputs cannot be directories.");
                }
            }

            foreach (var output in tesTask.Outputs ?? [])
            {
                if (string.IsNullOrWhiteSpace(output.Path) || !output.Path.StartsWith('/'))
                {
                    return BadRequest("Output paths in the container must be absolute paths.");
                }
            }

            tesTask.Tags ??= new Dictionary<string, string>(StringComparer.Ordinal); // case-sensitive is the default for Dictionary<string, T> as well as the default for JSON.
            tesTask.State = TesState.QUEUED;
            tesTask.CreationTime = DateTimeOffset.UtcNow;
            tesTask.TaskSubmitter = TaskSubmitter.Parse(tesTask);
            tesTask.Id = tesTask.CreateId();

            if (tesTask?.Resources is not null)
            {
                if (tesTask.Resources.CpuCores.HasValue && tesTask.Resources.CpuCores.Value <= 0)
                {
                    return BadRequest("cpu_cores must be greater than zero");
                }

                if (tesTask.Resources.DiskGb.HasValue && tesTask.Resources.DiskGb.Value <= 0.0)
                {
                    return BadRequest("disk_gb must be greater than zero");
                }

                if (tesTask.Resources.RamGb.HasValue && tesTask.Resources.RamGb.Value <= 0.0)
                {
                    return BadRequest("ram_gb must be greater than zero");
                }
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
                    logger.LogWarning("Unsupported keys were passed to TesResources.backend_parameters: '{UnsupportedKeys}'", string.Join(",", unsupportedKeys));
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

            logger.LogDebug("Creating task with id {TesTask} state {TesTaskState}", tesTask.Id, tesTask.State);
            tesTask = await repository.CreateItemAsync(tesTask, cancellationToken);
            taskScheduler.QueueTesTask(tesTask);
            return StatusCode(200, new TesCreateTaskResponse { Id = tesTask.Id });
        }

        /// <summary>
        /// GetServiceInfo provides information about the service, such as storage details, resource availability, and  other documentation.
        /// </summary>
        /// <response code="200"></response>
        [HttpGet]
        [Route("/v1/service-info")]
        [ValidateModelState]
        [SwaggerOperation("GetServiceInfo", "Provides information about the service, this structure is based on the standardized GA4GH service info structure. In addition, this endpoint will also provide information about customized storage endpoints offered by the TES server.")]
        [SwaggerResponse(statusCode: 200, type: typeof(TesServiceInfo), description: "")]
        public virtual IActionResult GetServiceInfo()
        {
            logger.LogInformation("Id: {ServiceInfoId} Name: {ServiceInfoName} Type: {ServiceInfoType} Description: {ServiceInfoDescription} Organization: {ServiceInfoOrganization} ContactUrl: {ServiceInfoContactUrl} DocumentationUrl: {ServiceInfoDocumentationUrl} CreatedAt:{ServiceInfoCreatedAt} UpdatedAt:{ServiceInfoUpdatedAt} Environment: {ServiceInfoEnvironment} Version: {ServiceInfoVersion} Storage: {ServiceInfoStorage} TesResourcesSupportedBackendParameters: {ServiceInfoTesResourcesSupportedBackendParameters}",
                serviceInfo.Id, serviceInfo.Name, serviceInfo.Type, serviceInfo.Description, serviceInfo.Organization, serviceInfo.ContactUrl, serviceInfo.DocumentationUrl, serviceInfo.CreatedAt, serviceInfo.UpdatedAt, serviceInfo.Environment, serviceInfo.Version, string.Join(",", serviceInfo.Storage ?? []), string.Join(",", serviceInfo.TesResourcesSupportedBackendParameters ?? []));
            return StatusCode(200, serviceInfo);
        }

        /// <summary>
        /// Get a task. TaskView is requested as such: \&quot;v1/tasks/{id}?view&#x3D;FULL\&quot;
        /// </summary>
        /// <param name="id">The id of the <see cref="TesTask"/> to get</param>
        /// <param name="view">OPTIONAL. Affects the fields included in the returned Task messages. See TaskView below.   - MINIMAL: Task message will include ONLY the fields:   Task.Id   Task.State  - BASIC: Task message will include all fields EXCEPT:   Task.ExecutorLog.stdout   Task.ExecutorLog.stderr   Input.content   TaskLog.system_logs  - FULL: Task message includes all fields.</param>
        /// <param name="cancellationToken">A<see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <response code="200"></response>
        [HttpGet]
        [Route("/v1/tasks/{id}")]
        [ValidateModelState]
        [SwaggerOperation("GetTask", "Get a single task, based on providing the exact task ID string.")]
        [SwaggerResponse(statusCode: 200, type: typeof(TesTask), description: "")]
        public virtual async Task<IActionResult> GetTaskAsync([FromRoute][Required] string id, [FromQuery] string view, CancellationToken cancellationToken)
        {
            if (!TesTask.IsValidId(id))
            {
                return BadRequest("Invalid ID");
            }

            TesView viewEnum;

            try
            {
                viewEnum = ParseView(view);
            }
            catch (ArgumentOutOfRangeException exc)
            {
                logger.LogError(exc, "{ErrorMessage}", exc.Message);
                return BadRequest(exc.Message);
            }

            TesTask tesTask = null;
            var itemFound = await repository.TryGetItemAsync(id, cancellationToken, item => tesTask = item);

            if (!itemFound)
            {
                return NotFound($"The task with id {id} does not exist.");
            }

            await TryRemoveItemFromCacheAsync(tesTask, view, cancellationToken);
            return TesJsonResult(tesTask, viewEnum);
        }

        /// <summary>
        /// List tasks. TaskView is requested as such: \&quot;v1/tasks?view&#x3D;BASIC\&quot;
        /// </summary>
        /// <param name="namePrefix">OPTIONAL. Filter the list to include tasks where the name matches this prefix. If unspecified, no task name filtering is done.</param>
        /// <param name="state">OPTIONAL. Filter tasks by state. If unspecified, no task state filtering is done.</param>
        /// <param name="tagKeys">OPTIONAL. Array of tag_key (see spec)</param>
        /// <param name="tagValues">OPTIONAL. Array of tag_value (see spec)</param>
        /// <param name="pageSize">OPTIONAL. Number of tasks to return in one page. Must be less than 2048. Defaults to 256.</param>
        /// <param name="pageToken">OPTIONAL. Page token is used to retrieve the next page of results. If unspecified, returns the first page of results. See ListTasksResponse.next_page_token</param>
        /// <param name="view">OPTIONAL. Affects the fields included in the returned Task messages. See TaskView below.   - MINIMAL: Task message will include ONLY the fields:   Task.Id   Task.State  - BASIC: Task message will include all fields EXCEPT:   Task.ExecutorLog.stdout   Task.ExecutorLog.stderr   Input.content   TaskLog.system_logs  - FULL: Task message includes all fields.</param>
        /// <param name="cancellationToken">A<see cref="CancellationToken"/> for controlling the lifetime of the asynchronous operation.</param>
        /// <response code="200"></response>
        [HttpGet]
        [Route("/v1/tasks")]
        [ValidateModelState]
        [SwaggerOperation("ListTasks", "List tasks tracked by the TES server. This includes queued, active and completed tasks. How long completed tasks are stored by the system may be dependent on the underlying implementation.")]
        [SwaggerResponse(statusCode: 200, type: typeof(TesListTasksResponse), description: "")]
        public virtual async Task<IActionResult> ListTasksAsync([FromQuery(Name = "name_prefix")] string namePrefix, [FromQuery] string state, [FromQuery(Name = "tag_key")] string[] tagKeys, [FromQuery(Name = "tag_value")] string[] tagValues, [FromQuery(Name = "page_size")] int? pageSize, [FromQuery(Name = "page_token")] string pageToken, [FromQuery] string view, CancellationToken cancellationToken)
        {
            TesState? stateEnum;

            try
            {
                stateEnum = string.IsNullOrEmpty(state) ? null : Enum.Parse<TesState>(state, true);
            }
            catch
            {
                logger.LogError(@"Invalid state parameter value. If provided, it must be one of: {StateAllowedValues}", string.Join(", ", Enum.GetNames(typeof(TesState))));
                return BadRequest($"Invalid state parameter value. If provided, it must be one of: {string.Join(", ", Enum.GetNames(typeof(TesState)))}");
            }

            var decodedPageToken = pageToken is null ? null : Encoding.UTF8.GetString(Base64UrlTextEncoder.Decode(pageToken));

            if (pageSize.HasValue && (pageSize < 1 || pageSize > 2047))
            {
                logger.LogError(@"pageSize invalid {InvalidPageSize}", pageSize);
                return BadRequest("Invalid page_size parameter value. If provided, pageSize must be greater than 0 and less than 2048. Defaults to 256.");
            }

            IDictionary<string, string> tags = null;

            if (tagKeys is not null && tagKeys.Length > 0 || tagValues is not null && tagValues.Length > 0)
            {
                if (tagKeys?.Length > 0)
                {
                    var (zippedTags, error) = ParseZippedTagsFromQuery();

                    if (string.IsNullOrEmpty(error))
                    {
                        tags = zippedTags;
                    }
                    else if (tagKeys.Length >= tagValues?.Length)
                    {
                        logger.LogWarning("Using backup method to parse tag filters.");
                        try
                        {
                            tags = tagKeys.Zip(tagValues.Concat(Enumerable.Repeat(string.Empty, tagKeys.Length - (tagValues?.Length ?? 0))), (Key, Value) => (Key, Value))
                                .ToDictionary(x => x.Key, x => x.Value ?? string.Empty, StringComparer.Ordinal);
                        }
                        catch (ArgumentException ex)
                        {
                            logger.LogError(ex, "Duplicated tag_key");
                            return BadRequest("Duplicated tag_key");
                        }
                    }
                    else
                    {
                        logger.LogError(@"Parsing tag filters resulted in error: {TagFilterError}", error);
                        return BadRequest(error);
                    }
                }
                else
                {
                    logger.LogError("tag_value query arguments found without tag_key query arguments.");
                    return BadRequest("Invalid tag_value parameter value. Only valid with paired tag_key parameter values.");
                }
            }

            var (rawPredicate, predicates) = GenerateSearchPredicates(repository, stateEnum, namePrefix, tags);

            TesView viewEnum;

            try
            {
                viewEnum = ParseView(view);
            }
            catch (ArgumentOutOfRangeException exc)
            {
                logger.LogError(exc, "{ErrorMessage}", exc.Message);
                return BadRequest(exc.Message);
            }

            IRepository<TesTask>.GetItemsResult result;

            try
            {
                result = await repository.GetItemsAsync(
                    decodedPageToken,
                    pageSize.HasValue ? (int)pageSize : 256,
                    cancellationToken,
                    rawPredicate,
                    predicates);
            }
            catch (ArgumentException ex)
            {
                return BadRequest(ex.Message);
            }

            var (nextPageToken, tasks) = result;

            var encodedNextPageToken = nextPageToken is null ? null : Base64UrlTextEncoder.Encode(Encoding.UTF8.GetBytes(nextPageToken));
            var response = new TesListTasksResponse { Tasks = tasks.ToList(), NextPageToken = encodedNextPageToken };

            return TesJsonResult(response, viewEnum);
        }

        // ?tag_key=foo1&tag_value=bar1&tag_key=foo2&tag_value=bar2
        private (IDictionary<string, string> Tags, string Error) ParseZippedTagsFromQuery()
        {
            // Per spec, every tag_value parameter must be preceded by exactly one corresponding tag_key parameter, and no tag_key parameter can be followed by more than one tag_value parameter.
            // As a result, there can be a total number of tag_key parameters that are greater than the number of tag_value parameters. It's not legal for there to be more tag_value parameters.

            var list = new List<(string Key, string Value)>();

            foreach (var encodedPair in new QueryStringEnumerable(Request.QueryString.Value))
            {
                // Note: Microsoft's ASP.NET considers the names of items in the query string to be case insensitive, although the HTTP spec declares that they are normally considered to be case sensitive.
                var name = new string(encodedPair.DecodeName().Span);

                switch (name)
                {
                    case var x when "tag_key".Equals(x, StringComparison.OrdinalIgnoreCase):
                        list.Add(new(new(encodedPair.DecodeValue().Span), null));
                        break;

                    case var x when "tag_value".Equals(x, StringComparison.OrdinalIgnoreCase):
                        if (list.Count == 0 || list[^1].Value is not null)
                        {
                            return (null, "Unmatched tag_value");
                        }

                        list[^1] = (list[^1].Key, new(encodedPair.DecodeValue().Span));
                        break;
                }
            }

            var tags = new Dictionary<string, string>(StringComparer.Ordinal);

            foreach (var (Key, Value) in list)
            {
                if (tags.ContainsKey(Key))
                {
                    return (null, "Duplicated tag_key");
                }

                tags.Add(Key, Value ?? string.Empty);
            }

            return (tags, null);
        }

        internal static (FormattableString RawPredicate, IEnumerable<System.Linq.Expressions.Expression<Func<TesTask, bool>>> EFPredicates) GenerateSearchPredicates(IRepository<TesTask> repository, TesState? state, string namePrefix, IDictionary<string, string> tags)
        {
            tags ??= new Dictionary<string, string>();
            List<System.Linq.Expressions.Expression<Func<TesTask, bool>>> efPredicates = null;
            AppendableFormattableString rawPredicate = null;

            if (!string.IsNullOrWhiteSpace(namePrefix))
            {
                EnsureEfPredicates(ref efPredicates).Add(t => t.Name.StartsWith(namePrefix));
            }

            if (state is not null)
            {
                EnsureEfPredicates(ref efPredicates).Add(t => t.State == state);
            }

            foreach (var tag in tags)
            {
                if (string.IsNullOrEmpty(tag.Value))
                {
                    rawPredicate = AndString(rawPredicate, new(repository.JsonFormattableRawString(nameof(TesTask.Tags), $" ? {tag.Key}")));
                }
                else
                {
                    rawPredicate = AndString(rawPredicate, new(repository.JsonFormattableRawString(nameof(TesTask.Tags), $"->>{tag.Key} = {tag.Value}")));
                }
            }

            return (rawPredicate, efPredicates);

            static List<System.Linq.Expressions.Expression<Func<TesTask, bool>>> EnsureEfPredicates(ref List<System.Linq.Expressions.Expression<Func<TesTask, bool>>> efPredicates)
                => efPredicates ??= [];

            static AppendableFormattableString AndString(AppendableFormattableString source, AppendableFormattableString additional)
            {
                if (source is null)
                {
                    return additional;
                }
                else
                {
                    source.Append($" AND ");
                    source.Append(additional);
                    return source;
                }
            }
        }

        private async ValueTask<bool> TryRemoveItemFromCacheAsync(TesTask tesTask, string view, CancellationToken cancellationToken)
        {
            try
            {
                if (tesTask.State == TesState.COMPLETE
                   || tesTask.State == TesState.CANCELED
                   || ((tesTask.State == TesState.SYSTEM_ERROR || tesTask.State == TesState.EXECUTOR_ERROR)
                        && Enum.TryParse<TesView>(view, true, out var tesView)
                        && tesView == TesView.FULL))
                {
                    // Cache optimization:
                    // If a task completed/canceled with no errors, Cromwell will not call again
                    // OR if the task failed with an error, Cromwell will call a second time requesting FULL view, at which point can remove from cache
                    return await repository.TryRemoveItemFromCacheAsync(tesTask, cancellationToken);
                }
            }
            catch (Exception exc)
            {
                // Do not re-throw, since a cache issue should not fail the GET request
                logger.LogWarning(exc, "An exception occurred while trying to remove TesTask with ID {TesTask} with view {TesTaskView} from the cache", tesTask.Id, view);
            }

            return false;
        }

        private static TesView ParseView(string view)
        {
            try
            {
                return string.IsNullOrEmpty(view) ? TesView.MINIMAL : Enum.Parse<TesView>(view, true);
            }
            catch
            {
                throw new ArgumentOutOfRangeException(nameof(view), $"Invalid view parameter value. If provided, it must be one of: {string.Join(", ", Enum.GetNames(typeof(TesView)))}");
            }
        }

        private static IActionResult TesJsonResult(object value, TesView viewEnum)
        {
            return new JsonResult(value, TesJsonSerializerSettings[viewEnum]) { StatusCode = 200 };
        }

        private sealed class AppendableFormattableString : FormattableString
        {
            private readonly List<FormattableString> strings = [];

            public AppendableFormattableString(FormattableString formattable)
            {
                ArgumentNullException.ThrowIfNull(formattable);
                strings.Add(formattable);
            }

            internal void Append(FormattableString formattable)
            {
                ArgumentNullException.ThrowIfNull(formattable);
                strings.Add(formattable);
            }

            public override int ArgumentCount => strings.Sum(s => s.ArgumentCount);

            public override string Format => string.Join(string.Empty, strings.Select(MungeFormat));

            // Alter index values in the format string to match its position in the composite Format
            private string MungeFormat(FormattableString formattable, int index)
            {
                var baseIndex = strings.Take(index).Sum(s => s.ArgumentCount);
                var format = formattable.Format;

                var parts = format.Split('{');
                return string.Join('{', parts.Select(MungePart));

                // Add baseIndex to the reference
                string MungePart(string partFormat, int partIndex)
                {
                    if (partIndex == 0)
                    {
                        return partFormat;
                    }

                    if (parts[partIndex - 1].Length == 0 && partFormat.Length == 0)
                    {
                        return partFormat;
                    }

                    var end = partFormat.IndexOfAny([',', ':', '}']);

                    if (end == -1)
                    {
                        return partFormat;
                    }

                    if (int.TryParse(partFormat.AsSpan(0, end), System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var item))
                    {
                        return string.Concat((item + baseIndex).ToString("D", System.Globalization.CultureInfo.InvariantCulture), partFormat.AsSpan(end));
                    }

                    return partFormat;
                }
            }

            public override object GetArgument(int index)
            {
                ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, ArgumentCount);

                foreach (var s in strings)
                {
                    if (s.ArgumentCount < index)
                    {
                        index -= s.ArgumentCount;
                        continue;
                    }

                    return s.GetArgument(index);
                }

                return null;
            }

            public override object[] GetArguments()
            {
                return strings.SelectMany(s => s.GetArguments()).ToArray();
            }

            public override string ToString(IFormatProvider formatProvider)
            {
                return string.Format(formatProvider, Format, GetArguments());
            }
        }
    }
}
