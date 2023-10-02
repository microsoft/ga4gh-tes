// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text.Json.Serialization;

namespace Tes.Runner.Events;

public class EventMessage
{
    [JsonPropertyName("event_id")]
    public string Id { get; set; } = null!;

    [JsonPropertyName("event_name")]
    public string Name { get; set; } = null!;

    [JsonPropertyName("status_message")]
    public string StatusMessage { get; set; } = null!;

    [JsonPropertyName("entity_type")]
    public string EntityType { get; set; } = null!;

    [JsonPropertyName("correlation_id")]
    public string CorrelationId { get; set; } = null!;

    [JsonPropertyName("entity_id")]
    public string EntityId { get; set; } = null!;

    [JsonPropertyName("resources")]
    public List<string>? Resources { get; set; }
    [JsonPropertyName("created")]
    public DateTime Created { get; set; }
    [JsonPropertyName("event_version")]
    public string EventVersion { get; set; } = null!;

    [JsonPropertyName("event_data_version")]
    public string EventDataVersion { get; set; } = null!;

    [JsonPropertyName("event_data")]
    public Dictionary<string, string>? EventData { get; set; }
}
