// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;

namespace Tes.Runner.Transfer
{
    public class Md5HashListProvider(ILogger logger) : IHashListProvider
    {
        private readonly ILogger logger = logger;
        private readonly ConcurrentDictionary<int, string> hashesDictionary = new();
        public const int HashListItemBlockSizeInBytes = BlobSizeUtils.BlockSizeIncrementUnitInBytes;

        public ConcurrentDictionary<int, string> HashList => hashesDictionary;

        public string CalculateAndAddBlockHash(PipelineBuffer pipelineBuffer)
        {
            var hash = CreateBufferHashList(pipelineBuffer);

            if (!hashesDictionary.TryAdd(pipelineBuffer.Ordinal, hash))
            {
                //this is an assertion, should not throw in any case....
                throw new InvalidOperationException("The hash for this part already exists.");
            }

            return hash;
        }

        public string GetRootHash()
        {
            var blockHashList = new StringBuilder();

            foreach (var key in hashesDictionary.Keys.Order())
            {
                hashesDictionary.TryGetValue(key, out var value);
                blockHashList.Append(value);
            }

            var data = Encoding.UTF8.GetBytes(blockHashList.ToString());

            var rootHash = CreateBlockMd5CheckSumValue(data, 0, data.Length);

            logger.LogInformation("Root Hash: {RootHash} set in property: {MetadataName}", rootHash, BlobApiHttpUtils.RootHashMetadataName);

            return rootHash;
        }


        private static string CreateBlockMd5CheckSumValue(byte[] buffer, int offset, int length)
        {
            return BitConverter.ToString(MD5.HashData(buffer.AsSpan(offset, length))).Replace("-", string.Empty).ToLowerInvariant();
        }

        private static string CreateBufferHashList(PipelineBuffer pipelineBuffer)
        {
            var stringBuilder = new StringBuilder();
            for (var i = 0; i < pipelineBuffer.Length; i += BlobSizeUtils.BlockSizeIncrementUnitInBytes)
            {
                var blockLength = Math.Min(BlobSizeUtils.BlockSizeIncrementUnitInBytes, pipelineBuffer.Length - i);
                stringBuilder.Append(CreateBlockMd5CheckSumValue(pipelineBuffer.Data, i, blockLength));
            }
            return stringBuilder.ToString();
        }
    }
}
