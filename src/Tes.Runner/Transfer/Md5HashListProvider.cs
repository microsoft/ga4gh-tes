// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;

namespace Tes.Runner.Transfer
{
    public class Md5HashListProvider : IHashListProvider
    {
        private readonly ILogger logger = PipelineLoggerFactory.Create<Md5HashListProvider>();
        private readonly ConcurrentDictionary<int, string> hashesDictionary = new();
        public const int HashListItemBlockSizeInBytes = BlobSizeUtils.BlockSizeIncrementUnitInBytes;

        public ConcurrentDictionary<int, string> HashList => hashesDictionary;

        public string CalculateAndAddBlockHash(PipelineBuffer pipelineBuffer)
        {
            var hash = CreateBufferHashList(pipelineBuffer.Data);

            if (!hashesDictionary.TryAdd(pipelineBuffer.Ordinal, hash))
            {
                //this is an assertion, should not throw in any case....
                throw new InvalidOperationException("The hash for this part already exists.");
            }

            return hash;
        }

        public string GetRootHash()
        {
            var stringBuilder = new StringBuilder();

            foreach (var key in hashesDictionary.Keys.Order())
            {
                hashesDictionary.TryGetValue(key, out var value);
                stringBuilder.Append(value);
            }

            var data = Encoding.UTF8.GetBytes(stringBuilder.ToString());

            var rootHash = CreateBlockMd5CheckSumValue(data, 0, data.Length);

            logger.LogInformation($"Root Hash: {rootHash}");

            return rootHash;
        }


        private static string CreateBlockMd5CheckSumValue(byte[] buffer, int offset, int length)
        {
            using var md5Provider = MD5.Create();
            return BitConverter.ToString(md5Provider.ComputeHash(buffer, offset, length)).Replace("-", "").ToLowerInvariant();
        }

        private string CreateBufferHashList(byte[] buffer)
        {
            var stringBuilder = new StringBuilder();
            for (var i = 0; i < buffer.Length; i += BlobSizeUtils.BlockSizeIncrementUnitInBytes)
            {
                var blockLength = Math.Min(BlobSizeUtils.BlockSizeIncrementUnitInBytes, buffer.Length - i);
                stringBuilder.Append(CreateBlockMd5CheckSumValue(buffer, i, blockLength));
            }
            return stringBuilder.ToString();
        }
    }
}
