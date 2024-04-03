// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Reflection;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Tes.Models;
using System.Linq;

namespace Tes.TaskSubmitters
{
    /// <summary>
    /// Workflow engine task metadata.
    /// </summary>
    // Regarding TypeDiscriminatorPropertyName : https://github.com/dotnet/runtime/issues/72604#issuecomment-1544811970 (fix won't be present until system.text.json v9 preview 2)
    // tl;dr - System.Text.Json requires that the discriminator property be the first json property returned. postgres jsonb returns properties in name-length order (shorter before longer). Upshot: we use a very short json property name.
    [JsonPolymorphic(TypeDiscriminatorPropertyName = "$t", IgnoreUnrecognizedTypeDiscriminators = false, UnknownDerivedTypeHandling = JsonUnknownDerivedTypeHandling.FailSerialization)]
    [JsonDerivedType(typeof(UnknownTaskSubmitter), UnknownTaskSubmitter.SubmitterName)]
    [JsonDerivedType(typeof(CromwellTaskSubmitter), CromwellTaskSubmitter.SubmitterName)]
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
    /// Polymorphic type converter for <see cref="TaskSubmitter"/> with Newtonsoft.Json
    /// </summary>
    /// <remarks>
    /// This uses the metadata that <see cref="System.Text.Json"/> uses.
    /// It depends on the convention that the type discriminator property value is mirrored through <see cref="TaskSubmitter.Name"/> (aka all types give their discriminator value through the base type constructor).
    /// </remarks>
    internal sealed class JsonValueConverterTaskSubmitter : Newtonsoft.Json.JsonConverter
    {
        private static readonly ReadOnlyDictionary<string, Type> submittersByName;

        static JsonValueConverterTaskSubmitter()
        {
            submittersByName = typeof(TaskSubmitter)
                .GetCustomAttributes<JsonDerivedTypeAttribute>()
                .Select(a => new KeyValuePair<string, Type>((string)a.TypeDiscriminator, a.DerivedType))
                .ToDictionary(StringComparer.Ordinal)
                .AsReadOnly();
        }

        public override bool CanConvert(Type objectType)
        {
            return typeof(TaskSubmitter).IsAssignableFrom(objectType);
        }

        public override object ReadJson(Newtonsoft.Json.JsonReader reader, Type objectType, object existingValue, Newtonsoft.Json.JsonSerializer serializer)
        {
            if (reader.TokenType == Newtonsoft.Json.JsonToken.Null)
            {
                return default;
            }

            var obj = Newtonsoft.Json.Linq.JObject.Load(reader);

            var item = (TaskSubmitter)Activator.CreateInstance(
                submittersByName.TryGetValue((string)obj["Name"], out var type)
                ? type
                : throw new KeyNotFoundException());

            serializer.Populate(obj.CreateReader(), item);

            return item;
        }

        public override void WriteJson(Newtonsoft.Json.JsonWriter writer, object value, Newtonsoft.Json.JsonSerializer serializer)
        {
            serializer.Serialize(writer, value);
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
