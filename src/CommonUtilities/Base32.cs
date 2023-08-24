// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections;

namespace CommonUtilities
{
    /// <summary>
    /// Implementation of RFC 4648 Base32
    /// </summary>
    public static class Base32
    {
        private static readonly char[] Rfc4648Base32 = new[] { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '2', '3', '4', '5', '6', '7' };
        private const int GroupBitlength = 5;
        private const int BitsPerByte = 8;
        private const int LargestBitPosition = GroupBitlength - 1;

        /// <summary>
        /// Converts binary to Base32
        /// </summary>
        /// <param name="bytes">Data to convert.</param>
        /// <returns>RFC 4648 Base32 representation</returns>
        public static string ConvertToBase32(this byte[] bytes) // https://datatracker.ietf.org/doc/html/rfc4648#section-6
            => new string(new BitArray(bytes).Cast<bool>()
                    // The RFC 4648 Base32 algorithm requires that the bits of each byte be presented in MSB order, but BitArray on every platform presents them in LSB order.

                    // Reverse each byte's bits to convert the stream from LSB to MSB
                    .ConvertGroup(BitsPerByte,
                        (bit, _) => bit,
                        (bits) => bits.Reverse())
                    // Combine all the groups back into one stream of bits
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
        /// Converts each group (of fixed size) of items into a new item (last group may be smaller)
        /// </summary>
        /// <typeparam name="TSource">Type of source items</typeparam>
        /// <typeparam name="TGroupItem">Intermediate type. Represents each <typeparamref name="TSource"/> in the enumeration passed to <paramref name="groupResultFunc"/></typeparam>
        /// <typeparam name="TResult">Type of the resultant items</typeparam>
        /// <param name="ts">The source enumerable of type <typeparamref name="TSource"/>.</param>
        /// <param name="itemsPerGroup">The number of items in each group to create out of the entire enumeration. The last group may end up smaller.</param>
        /// <param name="groupMemberFunc">The function that prepares each <typeparamref name="TSource"/> into the <typeparamref name="TGroupItem"/> expected by <paramref name="groupResultFunc"/>. Its parameters are an item of type <typeparamref name="TSource"/> and the index of that item (starting from zero) within each group.</param>
        /// <param name="groupResultFunc">The function that creates the <typeparamref name="TResult"/> from each enumeration (of size &lt;= <paramref name="itemsPerGroup"/>) of <typeparamref name="TGroupItem"/> in the group.</param>
        /// <returns>An enumeration of <typeparamref name="TResult"/> from each of the groups.</returns>
        /// <remarks>This method groups each set of <paramref name="itemsPerGroup"/> <typeparamref name="TSource"/> in <paramref name="ts"/>, passing each <typeparamref name="TSource"/> through <paramref name="groupMemberFunc"/> and sending that enumeration to <paramref name="groupResultFunc"/>, returning the enumeration of all the group results.</remarks>
        private static IEnumerable<TResult> ConvertGroup<TSource, TGroupItem, TResult>(
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
