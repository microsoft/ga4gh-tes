// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Linq;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using System.Text.RegularExpressions;

namespace Tes.Models
{
    /// <summary>
    /// Workflow engine task metadata.
    /// </summary>
    // Regarding TypeDiscriminatorPropertyName : https://github.com/dotnet/runtime/issues/72604#issuecomment-1544811970 (fix won't be present until system.text.json v9 preview 2)
    // tl;dr - System.Text.Json requires that the discriminator property be the first json property returned. postgres jsonb returns properties in name-length order (shorter before longer). Upshot: we use a very short json property name.
    [JsonPolymorphic(TypeDiscriminatorPropertyName = "$t", IgnoreUnrecognizedTypeDiscriminators = false, UnknownDerivedTypeHandling = JsonUnknownDerivedTypeHandling.FailSerialization)]
    [JsonDerivedType(typeof(UnknownTaskSubmitter), "unknown")]
    [JsonDerivedType(typeof(CromwellTaskSubmitter), "cromwell")]
    public abstract partial class TaskSubmitter
    {
        /// <summary>
        /// Submitter engine name.
        /// </summary>
        [JsonIgnore]
        public abstract string Name { get; }

        /// <summary>
        /// Top level workflow identifier.
        /// </summary>
        [JsonPropertyName("workflowId")]
        public virtual string WorkflowId { get; set; }

        /// <summary>
        /// <paramref name="task"/> parser to determine workflow engine.
        /// </summary>
        /// <param name="task"><see cref="TesTask"/> submitted by a workflow engine.</param>
        /// <returns>Task metadata from workflow engine.</returns>
        public static TaskSubmitter Parse(TesTask task)
        {
            var result = Attempt(CromwellTaskSubmitter.Parse, task);
            // TODO: add more submitters here
            result ??= Attempt(UnknownTaskSubmitter.Parse, task);
            return result;

            static TaskSubmitter Attempt(Func<TesTask, TaskSubmitter> parser, TesTask task)
            {
                try
                {
                    return parser(task);
                }
                catch (Exception)
                {
                    return null;
                }
            }
        }
    }

    /// <summary>
    /// Unknown task workflow engine metadata.
    /// </summary>
    public class UnknownTaskSubmitter : TaskSubmitter
    {
        internal static new UnknownTaskSubmitter Parse(TesTask _)
        {
            return new();
        }

        public override string Name => "unknown";
    }

    /// <summary>
    /// Cromwell workflow engine metadata.
    /// </summary>
    public partial class CromwellTaskSubmitter : TaskSubmitter
    {
        /// <inheritdoc/>
        public override string Name => "cromwell";

        // examples: /cromwell-executions/test/daf1a044-d741-4db9-8eb5-d6fd0519b1f1/call-hello/execution/rc
        // examples: /cromwell-executions/test/daf1a044-d741-4db9-8eb5-d6fd0519b1f1/call-hello/test-subworkflow/b5227f73-f6e8-43be-8b18-520b1fd789b6/call-subworkflow/shard-8/execution/rc
        [GeneratedRegex("/*?/(.+)/([^/]+)/([0-9A-Fa-f]{8}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{12})/call-([^/]+)(?:/shard-([^/]+))?/execution/rc", RegexOptions.Singleline)]
        private static partial Regex CromwellPathRegex();

        [GeneratedRegex("(.*):[^:]*:[^:]*", RegexOptions.Singleline)]
        private static partial Regex GetCromwellTaskInstanceNameRegex();

        [GeneratedRegex(".*:([^:]*):[^:]*", RegexOptions.Singleline)]
        private static partial Regex GetCromwellShardRegex();

        [GeneratedRegex(".*:([^:]*)", RegexOptions.Singleline)]
        private static partial Regex GetCromwellAttemptRegex();

        private static readonly Regex cromwellTaskInstanceNameRegex = GetCromwellTaskInstanceNameRegex();
        private static readonly Regex cromwellShardRegex = GetCromwellShardRegex();
        private static readonly Regex cromwellAttemptRegex = GetCromwellAttemptRegex();
        private static readonly Regex cromwellPathRegex = CromwellPathRegex();

        internal static new CromwellTaskSubmitter Parse(TesTask task)
        {
            if (string.IsNullOrWhiteSpace(task.Description))
            {
                return null;
            }

            var descriptionWorkflowId = Guid.Parse(task.Description.Split(':')[0]);
            TesOutput rcOutput = default;
            var hasStdErrOutput = false;
            var hasStdOutOutput = false;

            foreach (var output in task.Outputs ?? [])
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

            if (hasStdErrOutput && hasStdOutOutput && rcOutput is not null && !rcOutput.Path.Contains('\n'))
            {
                var path = rcOutput.Path.Split('/');
                // path[0] <= string.Empty
                // path[1] <= cromwell execution directory
                // path[2] <= top workflow name
                // path[3] <= top workflow id

                var match = cromwellPathRegex.Match(rcOutput.Path);
                // match.Groups[1] <= execution directory path below root until last sub workflow name (not including beginning or ending '/')
                // match.Groups[2] <= final workflow name, possibly prefixed with parent workflow name separated by '-'
                // match.Groups[3] <= final workflow id
                // match.Groups[4] <= final task
                // match.Groups[5] <= final shard, if present

                if (match.Success && match.Captures.Count == 1 && match.Groups.Count == 6)
                {
                    var workflowName = path[2];
                    var workflowId = path[3];
                    var subWorkflowId = match.Groups[3].Value;

                    if (Guid.TryParse(subWorkflowId, out var workflowIdAsGuid) && descriptionWorkflowId.Equals(workflowIdAsGuid))
                    {
                        return new()
                        {
                            WorkflowId = workflowId,
                            WorkflowName = workflowName,
                            CromwellTaskInstanceName = cromwellTaskInstanceNameRegex.Match(task.Description).Groups[1].Value,
                            CromwellShard = int.TryParse(cromwellShardRegex.Match(task.Description).Groups[1].Value, out var shard) ? shard : null,
                            CromwellAttempt = int.TryParse(cromwellAttemptRegex.Match(task.Description).Groups[1].Value, out var attempt) ? attempt : null,
                            ExecutionDir = string.Join('/', path.Take(path.Length - 1))
                        };
                    }
                }
            }

            return null;
        }

        /// <summary>
        /// Workflow name.
        /// </summary>
        [JsonPropertyName("cromwellWorkflowName")]
        public string WorkflowName { get; init; }

        /// <summary>
        /// Cromwell task description without shard and attempt numbers
        /// </summary>
        [JsonPropertyName("cromwellTaskInstanceName")]
        public string CromwellTaskInstanceName { get; init; }

        /// <summary>
        /// Cromwell shard number
        /// </summary>
        [JsonPropertyName("cromwellShard")]
        public int? CromwellShard { get; init; }

        /// <summary>
        /// Cromwell attempt number
        /// </summary>
        [JsonPropertyName("cromwellAttempt")]
        public int? CromwellAttempt { get; init; }

        /// <summary>
        /// Cromwell task execution directory.
        /// </summary>
        [JsonPropertyName("cromwellExecutionDir")]
        public string ExecutionDir { get; init; }
    }
}
