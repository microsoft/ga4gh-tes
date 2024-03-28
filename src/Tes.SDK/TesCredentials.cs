// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.SDK
{
    /// <summary>
    /// TES service basic credentials
    /// </summary>
    /// <param name="TesHostname">TES service host</param>
    /// <param name="TesUsername">TES ingress username</param>
    /// <param name="TesPassword">TES ingres password</param>
    public record TesCredentials(string TesHostname, string TesUsername, string TesPassword)
    {
        private static readonly System.Text.Json.JsonSerializerOptions serializerOptions = new() { IncludeFields = true, PropertyNameCaseInsensitive = true };

        /// <summary>
        /// Serializes credentials
        /// </summary>
        /// <returns>Serialized credentials.</returns>
        public string Serialize() =>
            System.Text.Json.JsonSerializer.Serialize(this, serializerOptions);

        /// <summary>
        /// Deserializes credentials
        /// </summary>
        /// <param name="stream"><see cref="Stream"/> to read credentials from.</param>
        /// <returns>Credentials.</returns>
        /// <exception cref="InvalidOperationException"> when System.Text.Json.JsonSerializer.Deserialize returns <c>null</c>"/>.</exception>
        public static TesCredentials Deserialize(Stream stream) =>
            System.Text.Json.JsonSerializer.Deserialize<TesCredentials>(stream, serializerOptions) ?? throw new InvalidOperationException("Deserialization failed.");
    };
}
