// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Security.Cryptography;
using Microsoft.Extensions.Logging;

namespace Tes.Runner.Transfer
{
    public class FileHashProcessor
    {
        private readonly string sourceFileName;
        private Task<string>? hashTask;
        protected readonly ILogger Logger = PipelineLoggerFactory.Create<FileHashProcessor>();

        private FileHashProcessor(string sourceFileName)
        {
            this.sourceFileName = sourceFileName;
        }


        private void StartMd5Processing()
        {
            hashTask = Task.Run(CalculateMd5FromFile);
        }

        private void StartBlake3Processing()
        {
            hashTask = Task.Run(CalculateBlakeFromFile);
        }

        public static FileHashProcessor StartNewMd5Processor(string sourceFileName)
        {
            ArgumentException.ThrowIfNullOrEmpty(sourceFileName);

            var processor = new FileHashProcessor(sourceFileName);

            processor.StartMd5Processing();

            return processor;
        }

        public static FileHashProcessor StartNewBlake5Processor(string sourceFileName)
        {
            ArgumentException.ThrowIfNullOrEmpty(sourceFileName);

            var processor = new FileHashProcessor(sourceFileName);

            processor.StartBlake3Processing();

            return processor;
        }

        private string CalculateMd5FromFile()
        {
            var buffer = new byte[BlobSizeUtils.MiB * 100];
            using var md5 = MD5.Create();
            using var stream = File.OpenRead(sourceFileName);
            int read;
            while ((read = stream.Read(buffer, 0, buffer.Length)) != 0)
            {
                Logger.LogInformation($"Calculating MD5 Block. Bytes read: {buffer.Length}");
                md5.TransformBlock(buffer, 0, read, buffer, 0);
            }

            md5.TransformFinalBlock(buffer, 0, 0);

            return BitConverter.ToString(md5.Hash!).Replace("-", "").ToLowerInvariant();
        }

        private string CalculateBlakeFromFile()
        {
            var buffer = new byte[BlobSizeUtils.MiB * 100];
            using var blake = Blake3.Hasher.New();
            using var stream = File.OpenRead(sourceFileName);
            int read;
            while ((read = stream.Read(buffer, 0, buffer.Length)) != 0)
            {
                Logger.LogInformation($"Calculating Blake Block. Bytes read: {buffer.Length}");
                blake.Update(buffer[0..read]);
            }

            var hash = blake.Finalize().ToString();

            return hash;
        }

        public async Task<string> GetFileMd5HashAsync()
        {
            if (hashTask is null)
            {
                throw new InvalidOperationException("The MD5 task has not been started.");
            }
            return await hashTask;
        }
    }
}
