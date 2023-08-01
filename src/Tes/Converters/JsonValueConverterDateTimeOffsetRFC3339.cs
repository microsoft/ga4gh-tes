// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Tes.Converters
{
    internal sealed class JsonValueConverterDateTimeOffsetRFC3339 : Newtonsoft.Json.Converters.IsoDateTimeConverter
    {
    }
}
