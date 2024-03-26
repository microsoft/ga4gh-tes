// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Converters
{
    internal sealed class JsonValueConverterDateTimeOffsetRFC3339_Newtonsoft : Newtonsoft.Json.Converters.IsoDateTimeConverter
    {
        public override void WriteJson(Newtonsoft.Json.JsonWriter writer, object value, Newtonsoft.Json.JsonSerializer serializer)
        {
            if (value is not System.DateTimeOffset datetimeOffset)
            {
                throw new Newtonsoft.Json.JsonSerializationException($"Unexpected value when converting date. Expected DateTimeOffset, got {value?.GetType().FullName ?? "<null>"}.");
            }

            writer.WriteValue(Format(datetimeOffset));
        }

        internal static string Format(System.DateTimeOffset value) => value.UtcDateTime.ToString("O");
    }

    internal sealed class JsonValueConverterDateTimeOffsetRFC3339_JsonText : System.Text.Json.Serialization.JsonConverter<System.DateTimeOffset>
    {
        public override System.DateTimeOffset Read(ref System.Text.Json.Utf8JsonReader reader, System.Type typeToConvert, System.Text.Json.JsonSerializerOptions options)
        {
            return reader.GetDateTimeOffset();
        }

        public override void Write(System.Text.Json.Utf8JsonWriter writer, System.DateTimeOffset value, System.Text.Json.JsonSerializerOptions options)
        {
            writer.WriteStringValue(JsonValueConverterDateTimeOffsetRFC3339_Newtonsoft.Format(value));
        }
    }
}
