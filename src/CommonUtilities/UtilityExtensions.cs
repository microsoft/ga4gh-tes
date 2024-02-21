// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections;

namespace CommonUtilities
{
    public static class UtilityExtensions
    {
        #region RFC 4648 Base32
        private static readonly char[] Rfc4648Base32 = new[] { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '2', '3', '4', '5', '6', '7' };
        private const int GroupBitlength = 5;
        private const int BitsPerByte = 8;
        private const int LargestBitPosition = GroupBitlength - 1;

        /// <summary>
        /// Converts binary to Base32
        /// </summary>
        /// <param name="bytes">Data to convert.</param>
        /// <returns>RFC 4648 Base32 representation</returns>
        /// <remarks>https://datatracker.ietf.org/doc/html/rfc4648#section-6</remarks>
        public static string ConvertToBase32(this byte[] bytes)

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
        #endregion

        /// <summary>
        /// Converts each group (by count) of items into a new item
        /// </summary>
        /// <typeparam name="TSource">Type of source items</typeparam>
        /// <typeparam name="TGroup">Intermediate type</typeparam>
        /// <typeparam name="TResult">Type of the resultant items</typeparam>
        /// <param name="source">The source enumerable of type <typeparamref name="TSource"/>.</param>
        /// <param name="groupSize">The size of each group to create out of the entire enumeration. The last group may be smaller.</param>
        /// <param name="source2Group">The function that prepares each <typeparamref name="TSource"/> into the value expected by <paramref name="group2Result"/>. Its parameters are an item of type <typeparamref name="TSource"/> and the index of that item (starting from zero) within each group.</param>
        /// <param name="group2Result">The function that creates the <typeparamref name="TResult"/> from each group of <typeparamref name="TGroup"/> items.</param>
        /// <returns>An enumeration of <typeparamref name="TResult"/> from all of the groups.</returns>
        public static IEnumerable<TResult> ConvertGroup<TSource, TGroup, TResult>(
            this IEnumerable<TSource> source,
            int groupSize,
            Func<TSource, int, TGroup> source2Group,
            Func<IEnumerable<TGroup>, TResult> group2Result)
            => source
                .Select((value, index) => (Index: index, Value: value))
                .GroupBy(tuple => tuple.Index / groupSize)
                .OrderBy(tuple => tuple.Key)
                .Select(groups => group2Result(groups.Select(item => source2Group(item.Value, item.Index % groupSize))));

        #region AddRange
        public static void AddRange<T>(this IList<T> list, IEnumerable<T> values)
        {
            foreach (var value in values)
            {
                list.Add(value);
            };
        }

        public static void AddRange<Tkey, TValue>(this IDictionary<Tkey, TValue> dictionary, IDictionary<Tkey, TValue> values)
        {
            foreach (var value in values)
            {
                dictionary.Add(value);
            };
        }
        #endregion
    }
}
