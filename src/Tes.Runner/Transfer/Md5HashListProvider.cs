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

        public ConcurrentDictionary<int, string> HashList => hashesDictionary;

        public string AddBlockHash(PipelineBuffer pipelineBuffer)
        {
            var hash = CreateBlockMd5CheckSumValue(pipelineBuffer.Data, pipelineBuffer.Length);

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

            var rootHash = CreateBlockMd5CheckSumValue(data, data.Length);

            logger.LogInformation($"Root Hash: {rootHash}");

            return rootHash;
        }

        private static string CreateBlockMd5CheckSumValue(byte[] buffer, int length)
        {
            using var md5Provider = MD5.Create();
            return BitConverter.ToString(md5Provider.ComputeHash(buffer, 0, length)).Replace("-", "").ToLowerInvariant();
        }
    }
}
