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
    }
}
