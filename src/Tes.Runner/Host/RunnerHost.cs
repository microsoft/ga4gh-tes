// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Buffers;

namespace Tes.Runner.Host
{
    public interface IRunnerHost
    {
        /// <summary>
        /// Metadata of shared metadata file.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        FileInfo GetSharedFile(string name);

        /// <summary>
        /// Saves content of shared metadata file.
        /// </summary>
        /// <param name="name">File name.</param>
        /// <param name="content">File content.</param>
        void WriteSharedFile(string name, ReadOnlySpan<byte> content);

        /// <summary>
        /// Loads content of shared metadata file.
        /// </summary>
        /// <param name="name">File name.</param>
        /// <returns>File content.</returns>
        IMemoryOwner<byte>? ReadSharedFile(string name);

        /// <summary>
        /// Ensure previous tasks working and metadata directories are removed.
        /// </summary>
        /// <returns></returns>
        Task NodeCleanupPreviousTasksAsync();

        //void WriteMetric(string key);

        //void WriteMetric(string key, string value);
    }

    internal abstract class RunnerHost : IRunnerHost
    {
        /// <inheritdoc/>
        public abstract FileInfo GetSharedFile(string name);

        /// <inheritdoc/>
        public abstract Task NodeCleanupPreviousTasksAsync();

        ///// <inheritdoc/>
        //public abstract void WriteMetric(string key, string value);

        ///// <inheritdoc/>
        //public void WriteMetric(string key)
        //    => WriteMetric(key, DateTimeOffset.UtcNow.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'sszzz", System.Globalization.CultureInfo.InvariantCulture));

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
