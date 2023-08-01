// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Tes.Converters
{
    internal sealed class JsonValueConverterDateTimeOffsetRFC3339 : Newtonsoft.Json.Converters.IsoDateTimeConverter
    {
        public override void WriteJson(JsonWriter writer, object value, Newtonsoft.Json.JsonSerializer serializer)
        {
            if (value is not DateTimeOffset)
            {
                throw new JsonSerializationException($"Unexpected value when converting date. Expected DateTime or DateTimeOffset, got {value?.GetType().FullName ?? "<null>"}.");
            }

            var dateTimeOffset = (DateTimeOffset)value;
            dateTimeOffset = dateTimeOffset.ToUniversalTime();
            writer.WriteValue(dateTimeOffset.ToString("O"));
        }
    }
}
