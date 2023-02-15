using System.Collections;

namespace CommonUtilities
{
    public static class Base32
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
                    (((data & 1 << 0) == 0) ? 0 : 1 << 7) |
                    (((data & 1 << 1) == 0) ? 0 : 1 << 6) |
                    (((data & 1 << 2) == 0) ? 0 : 1 << 5) |
                    (((data & 1 << 3) == 0) ? 0 : 1 << 4) |
                    (((data & 1 << 4) == 0) ? 0 : 1 << 3) |
                    (((data & 1 << 5) == 0) ? 0 : 1 << 2) |
                    (((data & 1 << 6) == 0) ? 0 : 1 << 1) |
                    (((data & 1 << 7) == 0) ? 0 : 1 << 0));
        }
    }
}
