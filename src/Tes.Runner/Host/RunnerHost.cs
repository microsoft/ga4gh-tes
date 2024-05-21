// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Buffers;

namespace Tes.Runner.Host
{
    public interface IRunnerHost
    {
        /// <summary>
        /// tbd
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        FileInfo GetSharedFile(string name);

        /// <summary>
        /// tbd
        /// </summary>
        /// <param name="name"></param>
        /// <param name="content"></param>
        void WriteSharedFile(string name, ReadOnlySpan<byte> content);

        /// <summary>
        /// tbd
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        IMemoryOwner<byte>? ReadSharedFile(string name);

        /// <summary>
        /// tbd
        /// </summary>
        /// <returns></returns>
        Task NodeCleanupAsync();
    }

    internal abstract class RunnerHost : IRunnerHost
    {
        /// <inheritdoc/>
        public abstract FileInfo GetSharedFile(string name);

        /// <inheritdoc/>
        public abstract Task NodeCleanupAsync();

        /// <inheritdoc/>
        IMemoryOwner<byte>? IRunnerHost.ReadSharedFile(string name)
        {
            var file = GetSharedFile(name);
            return file.Exists
                ? ReadFile(file.OpenRead())
                : null;

            static IMemoryOwner<byte> ReadFile(FileStream stream)
            {
                if (stream.Length > int.MaxValue)
                {
                    throw new ArgumentException("File is too large.", nameof(stream));
                }

                try
                {
                    var buffer = MemoryPool<byte>.Shared.Rent((int)stream.Length);
                    stream.Read(buffer.Memory.Span);
                    return buffer;
                }
                finally
                {
                    stream.Dispose();
                }
            }
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        void IRunnerHost.WriteSharedFile(string name, ReadOnlySpan<byte> content)
        {
            var file = GetSharedFile(name);
            file.Directory!.Create();
            using var stream = file.OpenWrite();
            stream.Write(content);
        }
    }
}
