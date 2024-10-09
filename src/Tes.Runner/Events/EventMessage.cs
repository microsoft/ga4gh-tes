// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json.Serialization;

namespace Tes.Runner.Events;

public sealed class EventMessage
{
    [JsonPropertyName("eventId")]
    public string Id { get; set; } = null!;

    [JsonPropertyName("eventName")]
    public string Name { get; set; } = null!;

    [JsonPropertyName("statusMessage")]
    public string StatusMessage { get; set; } = null!;

    [JsonPropertyName("entityType")]
    public string EntityType { get; set; } = null!;

    [JsonPropertyName("correlationId")]
    public string CorrelationId { get; set; } = null!;

    [JsonPropertyName("entityId")]
    public string EntityId { get; set; } = null!;

    [JsonPropertyName("resources")]
    public List<string>? Resources { get; set; }

    [JsonPropertyName("created")]
    public DateTime Created { get; set; }

    [JsonPropertyName("eventVersion")]
    public string EventVersion { get; set; } = null!;

    [JsonPropertyName("eventDataVersion")]
    public string EventDataVersion { get; set; } = null!;

    [JsonPropertyName("eventData")]
    public Dictionary<string, string>? EventData { get; set; }
}

[JsonSerializable(typeof(EventMessage))]
public partial class EventMessageContext : JsonSerializerContext
{ }
