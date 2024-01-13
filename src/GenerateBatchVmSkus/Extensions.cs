// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace GenerateBatchVmSkus
{
    internal static class Extensions
    {
        public static void ForEach<T>(this IEnumerable<T> values, Action<T> action)
        {
            ArgumentNullException.ThrowIfNull(values);
            ArgumentNullException.ThrowIfNull(action);

            foreach (var value in values)
            {
                action(value);
            }
        }

        public static bool Equals(this StringComparison comparison, string? a, string? b) => string.Equals(a, b, comparison);

        // TODO: better name than `Max`?
        public static TimeSpan Max(this TimeSpan reference, TimeSpan value)
        {
            return reference.CompareTo(value) switch
            {
                var x when x < 0 => value,
                var x when x == 0 => reference,
                var x when x > 0 => reference,
                _ => throw new System.Diagnostics.UnreachableException(),
            };
        }
    }
}
