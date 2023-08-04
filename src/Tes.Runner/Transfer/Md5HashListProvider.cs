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

            logger.LogInformation($"Root Hash: {rootHash} set in property: {BlobBlockApiHttpUtils.RootHashMetadataName}");

            return rootHash;
        }


        private static string CreateBlockMd5CheckSumValue(byte[] buffer, int offset, int length)
        {
            using var md5Provider = MD5.Create();
            return BitConverter.ToString(md5Provider.ComputeHash(buffer, offset, length)).Replace("-", "").ToLowerInvariant();
        }

        private string CreateBufferHashList(PipelineBuffer pipelineBuffer)
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
