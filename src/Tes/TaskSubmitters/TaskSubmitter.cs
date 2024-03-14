// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Tes.Models;

namespace Tes.TaskSubmitters
{
    /// <summary>
    /// Workflow engine task metadata.
    /// </summary>
    // Regarding TypeDiscriminatorPropertyName : https://github.com/dotnet/runtime/issues/72604#issuecomment-1544811970 (fix won't be present until system.text.json v9 preview 2)
    // tl;dr - System.Text.Json requires that the discriminator property be the first json property returned. postgres jsonb returns properties in name-length order (shorter before longer). Upshot: we use a very short json property name.
    [JsonPolymorphic(TypeDiscriminatorPropertyName = "$t", IgnoreUnrecognizedTypeDiscriminators = false, UnknownDerivedTypeHandling = JsonUnknownDerivedTypeHandling.FailSerialization)]
    [JsonDerivedType(typeof(UnknownTaskSubmitter), "unknown")]
    [JsonDerivedType(typeof(CromwellTaskSubmitter), "cromwell")]
    public abstract partial class TaskSubmitter(string name)
    {
        /// <summary>
        /// Submitter engine name.
        /// </summary>
        [JsonIgnore]
        public string Name { get; } = name;

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
        /// <remarks>Tries each known provider, returns the first one in the list that recognizes its metadata in the submitted <see cref="TesTask"/>.</remarks>
        public static TaskSubmitter Parse(TesTask task)
        {
            var result = Attempt(CromwellTaskSubmitter.Parse, task);
            // TODO: incorporate more submitters here
            result ??= Attempt(UnknownTaskSubmitter.Parse, task); // call this last
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
    public class UnknownTaskSubmitter() : TaskSubmitter(SubmitterName)
    {
        internal static new UnknownTaskSubmitter Parse(TesTask _)
        {
            return new();
        }

        public const string SubmitterName = "unknown";
    }
}
