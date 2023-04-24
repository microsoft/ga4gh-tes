// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections;

namespace CommonUtilities
{
    public static class Base32
    {
        private const int BitsPerByte = 8;
        private const int GroupBitlength = 5;
        private const int LargestBitPosition = GroupBitlength - 1;

        private static readonly char[] Rfc4648Base32 = new[] { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '2', '3', '4', '5', '6', '7' };
        private static readonly string[] Rfc4648Base32Suffix = new[] { string.Empty, @"======", @"====", @"===", @"=" };

        /// <summary>
        /// Converts binary to Base32
        /// </summary>
        /// <param name="bytes">Data to convert.</param>
        /// <returns>RFC 4648 Base32 representation</returns>
        public static string ConvertToBase32(byte[] bytes) // https://datatracker.ietf.org/doc/html/rfc4648#section-6

            // The RFC 4648 Base32 algorithm requires that each byte be presented in MSB order, but BitArray on every platform presents them in LSB order.
            => new string(new BitArray(bytes).Cast<bool>()

                    // Reverse each byte's bits to convert the stream from LSB to MSB
                    .ConvertByBatch(BitsPerByte,
                        (bit, _) => bit,
                        (bits) => bits.Reverse())
                    .SelectMany(b => b)

                    // Convert each 5-bit group in the stream into its final character
                    .ConvertByBatch(GroupBitlength,
                        (bit, index) => bit ? 1 << LargestBitPosition - index : 0,
                        (values) => Rfc4648Base32[values.Sum()])
                    .ToArray())

                // Append suffix
                + Rfc4648Base32Suffix[bytes.Length % GroupBitlength];

        /// <summary>
        /// Converts each batch (fixed number) of items from an emumeration into a new item
        /// </summary>
        /// <typeparam name="TSource">Type of source items</typeparam>
        /// <typeparam name="TBatchMemberItem">Intermediate type</typeparam>
        /// <typeparam name="TResult">Type of the resultant items</typeparam>
        /// <param name="ts">The source enumerable of type <typeparamref name="TSource"/>.</param>
        /// <param name="itemsPerBatch">The size of each batch to create out of the entire enumeration. The last batch may be smaller.</param>
        /// <param name="sourceToBatchMemberConverter">The function that prepares each <typeparamref name="TSource"/> into the type expected by <paramref name="batchToResultConverter"/>. Its parameters are an item of type <typeparamref name="TSource"/> and the <see cref="Int32"/> index of that item (starting from zero) within each group.</param>
        /// <param name="batchToResultConverter">The function that creates the <typeparamref name="TResult"/> from each batch of <typeparamref name="TBatchMemberItem"/>.</param>
        /// <returns>An enumeration of <typeparamref name="TResult"/> from all of the batches.</returns>
        public static IEnumerable<TResult> ConvertByBatch<TSource, TBatchMemberItem, TResult>(
            this IEnumerable<TSource> ts,
            int itemsPerBatch,
            Func<TSource, int, TBatchMemberItem> sourceToBatchMemberConverter,
            Func<IEnumerable<TBatchMemberItem>, TResult> batchToResultConverter)
            => ts
                .Select((value, index) => (Index: index, Value: value))
                .GroupBy(tuple => tuple.Index / itemsPerBatch)
                .OrderBy(tuple => tuple.Key)
                .Select(items => batchToResultConverter(items.Select(i => sourceToBatchMemberConverter(i.Value, i.Index % itemsPerBatch))));
    }
}
