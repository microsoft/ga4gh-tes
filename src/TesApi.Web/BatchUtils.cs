// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.IO;
using Newtonsoft.Json;

namespace TesApi.Web
{
    /// <summary>
    /// Util class for Azure Batch related helper functions.
    /// </summary>
    public static class BatchUtils
    {
        /// <summary>
        /// Deserializes the JSON structure contained by the specified System.IO.TextReader
        /// into an instance of the specified type.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="textReader"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public static T ReadJson<T>(TextReader textReader, Func<T> defaultValue)
        {
            return textReader is null
                ? defaultValue()
                : ReadJsonFile();

            T ReadJsonFile()
            {
                using var reader = new JsonTextReader(textReader);
                return JsonSerializer.CreateDefault().Deserialize<T>(reader) ?? defaultValue();
            }
        }

        /// <summary>
        /// Serializes the specified <typeparamref name="T"/> and writes the JSON structure
        /// into the returned <see cref="String"/>.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string WriteJson<T>(T value)
        {
            using var result = new StringWriter();
            using var writer = new JsonTextWriter(result);
            JsonSerializer.CreateDefault(new() { Error = (o, e) => throw e.ErrorContext.Error }).Serialize(writer, value);
            return result.ToString();
        }
    }
}
