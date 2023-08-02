// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Converters
{
    internal sealed class JsonValueConverterDateTimeOffsetRFC3339 : Newtonsoft.Json.Converters.IsoDateTimeConverter
    {
        public override void WriteJson(Newtonsoft.Json.JsonWriter writer, object value, Newtonsoft.Json.JsonSerializer serializer)
        {
            if (value is not System.DateTimeOffset)
            {
                throw new Newtonsoft.Json.JsonSerializationException($"Unexpected value when converting date. Expected DateTimeOffset, got {value?.GetType().FullName ?? "<null>"}.");
            }

            var dateTimeOffset = (System.DateTimeOffset)value;
            writer.WriteValue(dateTimeOffset.UtcDateTime.ToString("O"));
        }
    }
}
