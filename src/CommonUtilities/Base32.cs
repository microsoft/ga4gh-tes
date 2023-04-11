// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections;

namespace CommonUtilities
{
    public static class Base32
    {
        private static readonly char[] Rfc4648Base32 = new[] { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '2', '3', '4', '5', '6', '7' };
        const int GroupBitlength = 5;
        const int BitsPerByte = 8;
        const int LargestBitPosition = GroupBitlength - 1;

        /// <summary>
        /// Converts binary to Base32
        /// </summary>
        /// <param name="bytes">Data to convert.</param>
        /// <returns>RFC 4648 Base32 representation</returns>
        public static string ConvertToBase32(byte[] bytes) // https://datatracker.ietf.org/doc/html/rfc4648#section-6

            // The RFC 4648 Base32 algorithm requires that each byte be presented in MSB order, but BitArray on every platform presents them in LSB order.
            => new string(new BitArray(bytes).Cast<bool>()

                    // Reverse each byte's bits to convert the stream from LSB to MSB
                    .ConvertGroup(BitsPerByte,
                        (bit, _) => bit,
                        (bits) => bits.Reverse())
                    .SelectMany(b => b)

                    // Convert each 5-bit group in the stream into its final character
                    .ConvertGroup(GroupBitlength,
                        (bit, index) => bit ? 1 << LargestBitPosition - index : 0,
                        (values) => Rfc4648Base32[values.Sum()])
                    .ToArray())

                // Append suffix
                + (bytes.Length % GroupBitlength) switch
                {
                    0 => string.Empty,
                    1 => @"======",
                    2 => @"====",
                    3 => @"===",
                    4 => @"=",
                    _ => throw new InvalidOperationException(), // Keeps the compiler happy.
                };

        /// <summary>
        /// Converts each group (fixed number) of items into a new item
        /// </summary>
        /// <typeparam name="TSource">Type of source items</typeparam>
        /// <typeparam name="TGroupItem">Intermediate type</typeparam>
        /// <typeparam name="TResult">Type of the resultant items</typeparam>
        /// <param name="ts">The source enumerable of type <typeparamref name="TSource"/>.</param>
        /// <param name="itemsPerGroup">The size of each group to create out of the entire enumeration. The last group may be smaller.</param>
        /// <param name="groupMemberFunc">The function that prepares each <typeparamref name="TSource"/> into the value expected by <paramref name="groupResultFunc"/>. Its parameters are <typeparamref name="TSource"/> and the index of that item (starting from zero) within each grouping.</param>
        /// <param name="groupResultFunc">The function that creates the <typeparamref name="TResult"/> from each group of <typeparamref name="TGroupItem"/>.</param>
        /// <returns>An enumeration of <typeparamref name="TResult"/> from all of the groups.</returns>
        static IEnumerable<TResult> ConvertGroup<TSource, TGroupItem, TResult>(
            this IEnumerable<TSource> ts,
            int itemsPerGroup,
            Func<TSource, int, TGroupItem> groupMemberFunc,
            Func<IEnumerable<TGroupItem>, TResult> groupResultFunc)
            => ts
                .Select((value, index) => (Index: index, Value: value))
                .GroupBy(tuple => tuple.Index / itemsPerGroup)
                .OrderBy(tuple => tuple.Key)
                .Select(items => groupResultFunc(items.Select(i => groupMemberFunc(i.Value, i.Index % itemsPerGroup))));
    }
}
