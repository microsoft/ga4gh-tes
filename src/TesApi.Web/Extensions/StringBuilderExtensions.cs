// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Text;

namespace TesApi.Web.Extensions
{
    /// <summary>
    /// String builder extension
    /// </summary>
    public static class StringBuilderExtensions
    {
        /// <summary>
        /// Appends a line using a Linux line-ending
        /// </summary>
        /// <param name="builder">builder instance</param>
        /// <param name="value">value</param>
        /// <returns></returns>
        public static StringBuilder AppendLinuxLine(this StringBuilder builder, string value)
        {
            builder.Append(value);
            return builder.Append('\n');
        }
    }
}
