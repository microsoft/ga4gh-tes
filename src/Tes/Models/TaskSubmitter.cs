// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using System.Text.RegularExpressions;
using Tes.Extensions;

namespace Tes.Models
{
    /// <summary>
    /// Workflow engine task metadata.
    /// </summary>
    [JsonPolymorphic(TypeDiscriminatorPropertyName = "submitterName", IgnoreUnrecognizedTypeDiscriminators = false, UnknownDerivedTypeHandling = JsonUnknownDerivedTypeHandling.FailSerialization)]
    [JsonDerivedType(typeof(UnknownTaskSubmitter), "unknown")]
    [JsonDerivedType(typeof(CromwellTaskSubmitter), "cromwell")]
    public abstract partial class TaskSubmitter
    {
        // examples: /cromwell-executions/test/daf1a044-d741-4db9-8eb5-d6fd0519b1f1/call-hello/execution/rc
        // examples: /cromwell-executions/test/daf1a044-d741-4db9-8eb5-d6fd0519b1f1/call-hello/test-subworkflow/b5227f73-f6e8-43be-8b18-520b1fd789b6/call-subworkflow/shard-8/execution/rc
        [GeneratedRegex("/[^/]*?/([^/]+)/([0-9A-Fa-f]{8}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{12})/call-([^/]+)(?:/([^/]+)/([0-9A-Fa-f]{8}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{12})/call-([^/]+)/([^/]+))?/execution/rc", RegexOptions.Singleline)]
        private static partial Regex CromwellPathRegex();
        private static readonly Regex cromwellPathRegex = CromwellPathRegex();

        protected TaskSubmitter()
        { }

        protected TaskSubmitter(string workflow_id)
        {
            WorkflowId = workflow_id;
        }

        /// <summary>
        /// Submitter engine name.
        /// </summary>
        [JsonIgnore]
        public abstract string Name { get; }

        /// <summary>
        /// Top level workflow identifier.
        /// </summary>
        [JsonPropertyName("workflowId")]
        public virtual string WorkflowId { get; init; }

        /// <summary>
        /// <paramref name="task"/> parser to determine workflow engine.
        /// </summary>
        /// <param name="task"><see cref="TesTask"/> submitted by a workflow engine.</param>
        /// <returns>Task metadata from workflow engine.</returns>
        public static TaskSubmitter Parse(TesTask task)
        {
            // Check for cromwell
            try
            {
                var descriptionWorkflowId = Guid.Parse(task.Description.Split(':')[0]);
                TesOutput rcOutput = default;
                var hasStdErrOutput = false;
                var hasStdOutOutput = false;

                foreach (var output in task.Outputs)
                {
                    if (output.Path.EndsWith("/execution/rc"))
                    {
                        rcOutput = output;
                    }
                    else
                    {
                        hasStdErrOutput |= output.Path.EndsWith("/execution/stderr");
                        hasStdOutOutput |= output.Path.EndsWith("/execution/stdout");
                    }
                }

                if (rcOutput is not null && !rcOutput.Path.Contains('\n') && hasStdErrOutput && hasStdOutOutput)
                {
                    var match = cromwellPathRegex.Match(rcOutput.Path);

                    if (match.Success && 1 == match.Captures.Count && match.Groups.Count == 8)
                    {
                        var workflowName = match.Groups[1].Value;
                        var workflowId = match.Groups[2].Value;
                        var callName = match.Groups[3].Value;
                        var subWorkflowName = NullIfWhiteSpace(match.Groups[4].Value);
                        var subWorkflowId = NullIfWhiteSpace(match.Groups[5].Value);
                        var subCallName = NullIfWhiteSpace(match.Groups[6].Value);
                        var shard = NullIfWhiteSpace(match.Groups[7].Value);

                        if (Guid.TryParse(subWorkflowId ?? workflowId, out var result) && descriptionWorkflowId.Equals(result))
                        {
                            return new CromwellTaskSubmitter
                            {
                                WorkflowId = workflowId,
                                WorkflowName = workflowName,
                                WorkflowStage = callName,
                                SubWorkflowName = subWorkflowName,
                                SubWorkflowId = subWorkflowId,
                                SubWorkflowStage = subCallName,
                                Shard = shard,
                                ExecutionDir = Path.GetDirectoryName(rcOutput.Path)
                            };
                        }

                        static string NullIfWhiteSpace(string value) => string.IsNullOrWhiteSpace(value) ? null : value;
                    }
                }
            }
            catch (Exception) { }

            return new UnknownTaskSubmitter();
        }
    }

    internal sealed class TaskSubmitterTypeInfoResolver : DefaultJsonTypeInfoResolver
    {
        private static readonly Type taskSubitterType = typeof(TaskSubmitter);
        private static readonly JsonPolymorphicAttribute taskSubmitterJsonPolymorphic = taskSubitterType.GetCustomAttribute<JsonPolymorphicAttribute>();

        public override JsonTypeInfo GetTypeInfo(Type type, JsonSerializerOptions options)
        {
            var jsonTypeInfo = base.GetTypeInfo(type, options);

            if (taskSubitterType.Equals(jsonTypeInfo.Type))
            {
                jsonTypeInfo.PolymorphismOptions = new()
                {
                    TypeDiscriminatorPropertyName = taskSubmitterJsonPolymorphic.TypeDiscriminatorPropertyName,
                    IgnoreUnrecognizedTypeDiscriminators = taskSubmitterJsonPolymorphic.IgnoreUnrecognizedTypeDiscriminators,
                    UnknownDerivedTypeHandling = taskSubmitterJsonPolymorphic.UnknownDerivedTypeHandling
                };

                jsonTypeInfo.PolymorphismOptions.DerivedTypes.AddRange(
                    taskSubitterType.GetCustomAttributes<JsonDerivedTypeAttribute>().Select(a => new JsonDerivedType(a.DerivedType, (string)a.TypeDiscriminator)));
            }

            return jsonTypeInfo;
        }
    }

    /// <summary>
    /// Unknown task workflow engine metadata.
    /// </summary>
    public sealed class UnknownTaskSubmitter : TaskSubmitter
    {
        public UnknownTaskSubmitter()
        { }

        [JsonConstructor]
        public UnknownTaskSubmitter(string workflow_id)
            : base(workflow_id) { }

        public override string Name => "unknown";
    }

    /// <summary>
    /// Cromwell workflow engine metadata.
    /// </summary>
    public sealed class CromwellTaskSubmitter : TaskSubmitter
    {
        public CromwellTaskSubmitter()
        { }

        [JsonConstructor]
        public CromwellTaskSubmitter(string workflow_id)
            : base(workflow_id) { }

        /// <inheritdoc/>
        public override string Name => "cromwell";

        /// <summary>
        /// Workflow name.
        /// </summary>
        [JsonPropertyName("workflowName")]
        public string WorkflowName { get; init; }

        /// <summary>
        /// Workflow stage AKA WDL workflow task.
        /// </summary>
        [JsonPropertyName("workflowStage")]
        public string WorkflowStage { get; init; }

        /// <summary>
        /// Sub workflow name.
        /// </summary>
        [JsonPropertyName("subWorkflowName")]
        public string SubWorkflowName { get; init; }

        /// <summary>
        /// Sub workflow id.
        /// </summary>
        [JsonPropertyName("subWorkflowId")]
        public string SubWorkflowId { get; init; }

        /// <summary>
        /// Sub workflow stage.
        /// </summary>
        [JsonPropertyName("subWorkflowStage")]
        public string SubWorkflowStage { get; init; }

        /// <summary>
        /// Workflow shard.
        /// </summary>
        [JsonPropertyName("shard")]
        public string Shard { get; init; }

        /// <summary>
        /// Cromwell task execution directory.
        /// </summary>
        [JsonPropertyName("cromwellExecutionDir")]
        public string ExecutionDir { get; init; }
    }
}
