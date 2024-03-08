// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.IO;
using System.Runtime.Serialization;
using System.Text.RegularExpressions;

namespace Tes.Models
{
    /// <summary>
    /// Workflow engine task metadata.
    /// </summary>
    public abstract partial class TaskSubmitter
    {
        // examples: /cromwell-executions/test/daf1a044-d741-4db9-8eb5-d6fd0519b1f1/call-hello/execution/rc
        // examples: /cromwell-executions/test/daf1a044-d741-4db9-8eb5-d6fd0519b1f1/call-hello/test-subworkflow/b5227f73-f6e8-43be-8b18-520b1fd789b6/call-subworkflow/shard-8/execution/rc
        [GeneratedRegex("/[^/]*?/([^/]+)/([0-9A-Fa-f]{8}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{12})/call-([^/]+)(?:/([^/]+)/([0-9A-Fa-f]{8}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{12})/call-([^/]+)/([^/]+))?/execution/rc", RegexOptions.Singleline)]
        private static partial Regex CromwellPathRegex();
        private static readonly Regex cromwellPathRegex = CromwellPathRegex();

        protected TaskSubmitter()
        { }

        protected TaskSubmitter(string submitter_name, string workflow_id)
        {
            if (!Name.Equals(submitter_name, StringComparison.Ordinal))
            {
                throw new ArgumentException(null, nameof(submitter_name));
            }

            WorkflowId = workflow_id;
        }

        /// <summary>
        /// Submitter engine name.
        /// </summary>
        [DataMember(Name = "submitter_name")]
        public abstract string Name { get; }

        /// <summary>
        /// Top level workflow identifier.
        /// </summary>
        [DataMember(Name = "workflow_id")]
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

    /// <summary>
    /// Unknown task workflow engine metadata.
    /// </summary>
    public sealed class UnknownTaskSubmitter : TaskSubmitter
    {
        public UnknownTaskSubmitter()
        { }

        [Newtonsoft.Json.JsonConstructor]
        [System.Text.Json.Serialization.JsonConstructor]
        public UnknownTaskSubmitter(string submitter_name, string workflow_id)
            : base(submitter_name, workflow_id) { }

        public override string Name => "unknown";
    }

    /// <summary>
    /// Cromwell workflow engine metadata.
    /// </summary>
    public sealed class CromwellTaskSubmitter : TaskSubmitter
    {
        public CromwellTaskSubmitter()
        { }

        [Newtonsoft.Json.JsonConstructor]
        [System.Text.Json.Serialization.JsonConstructor]
        public CromwellTaskSubmitter(string submitter_name, string workflow_id)
            : base(submitter_name, workflow_id) { }

        /// <inheritdoc/>
        public override string Name => "cromwell";

        /// <summary>
        /// Workflow name.
        /// </summary>
        [DataMember(Name = "workflow_name")]
        public string WorkflowName { get; init; }

        /// <summary>
        /// Workflow stage AKA WDL workflow task.
        /// </summary>
        [DataMember(Name = "workflow_stage")]
        public string WorkflowStage { get; init; }

        /// <summary>
        /// Sub workflow name.
        /// </summary>
        [DataMember(Name = "sub_workflow_name")]
        public string SubWorkflowName { get; init; }

        /// <summary>
        /// Sub workflow id.
        /// </summary>
        [DataMember(Name = "sub_workflow_id")]
        public string SubWorkflowId { get; init; }

        /// <summary>
        /// Sub workflow stage.
        /// </summary>
        [DataMember(Name = "sub_workflow_stage")]
        public string SubWorkflowStage { get; init; }

        /// <summary>
        /// Workflow shard.
        /// </summary>
        [DataMember(Name = "shard")]
        public string Shard { get; init; }

        /// <summary>
        /// Cromwell task execution directory.
        /// </summary>
        [DataMember(Name = "cromwell_execution_dir")]
        public string ExecutionDir { get; init; }
    }
}
