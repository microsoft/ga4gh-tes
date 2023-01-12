// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections;
using System.IO;
using System.Linq;
using Newtonsoft.Json;

namespace TesApi.Web
{
    /// <summary>
    /// Util class for Azure Batch related helper functions.
    /// </summary>
    public static class BatchUtils
    {
        private static readonly char[] Rfc4648Base32 = new[] { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '2', '3', '4', '5', '6', '7' };

        /// <summary>
        /// Converts binary to Base32
        /// </summary>
        /// <param name="bytes">Data to convert.</param>
        /// <returns>RFC 4648 Base32 representation</returns>
        /// <exception cref="InvalidOperationException"></exception>
        public static string ConvertToBase32(byte[] bytes) // https://datatracker.ietf.org/doc/html/rfc4648#section-6
        {
            const int groupBitlength = 5;

            if (BitConverter.IsLittleEndian)
            {
                bytes = bytes.Select(FlipByte).ToArray();
            }

            return new string(new BitArray(bytes)
                    .Cast<bool>()
                    .Select((b, i) => (Index: i, Value: b ? 1 << (groupBitlength - 1 - (i % groupBitlength)) : 0))
                    .GroupBy(t => t.Index / groupBitlength)
                    .Select(g => Rfc4648Base32[g.Sum(t => t.Value)])
                    .ToArray())
                + (bytes.Length % groupBitlength) switch
                {
                    0 => string.Empty,
                    1 => @"======",
                    2 => @"====",
                    3 => @"===",
                    4 => @"=",
                    _ => throw new InvalidOperationException(), // Keeps the compiler happy.
                };

            static byte FlipByte(byte data)
                => (byte)(
                    (((data & 0x01) == 0) ? 0 : 0x80) |
                    (((data & 0x02) == 0) ? 0 : 0x40) |
                    (((data & 0x04) == 0) ? 0 : 0x20) |
                    (((data & 0x08) == 0) ? 0 : 0x10) |
                    (((data & 0x10) == 0) ? 0 : 0x08) |
                    (((data & 0x20) == 0) ? 0 : 0x04) |
                    (((data & 0x40) == 0) ? 0 : 0x02) |
                    (((data & 0x80) == 0) ? 0 : 0x01));
        }

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
