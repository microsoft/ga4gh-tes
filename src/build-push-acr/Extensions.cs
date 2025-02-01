// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace BuildPushAcr
{
    internal static class Extensions
    {
        public static async IAsyncEnumerable<System.Formats.Tar.TarEntry> GetEntriesAsync(this System.Formats.Tar.TarReader tarReader, bool copyData = false, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            System.Formats.Tar.TarEntry? entry;

            while ((entry = await tarReader.GetNextEntryAsync(copyData: copyData, cancellationToken: cancellationToken)) is not null)
            {
                yield return entry;
            }
        }

        public static System.Formats.Tar.TarEntry Clone(this System.Formats.Tar.TarEntry tarEntry)
        {
            System.Formats.Tar.TarEntry result = tarEntry.Format switch
            {
                var _ when System.Formats.Tar.TarEntryType.GlobalExtendedAttributes.Equals(tarEntry.EntryType) => new System.Formats.Tar.PaxGlobalExtendedAttributesTarEntry(((System.Formats.Tar.PaxGlobalExtendedAttributesTarEntry)tarEntry).GlobalExtendedAttributes),
                System.Formats.Tar.TarEntryFormat.Pax => new System.Formats.Tar.PaxTarEntry(tarEntry),
                System.Formats.Tar.TarEntryFormat.Ustar => new System.Formats.Tar.UstarTarEntry(tarEntry),
                System.Formats.Tar.TarEntryFormat.Gnu => new System.Formats.Tar.GnuTarEntry(tarEntry),
                System.Formats.Tar.TarEntryFormat.V7 => new System.Formats.Tar.V7TarEntry(tarEntry),
                _ => throw new NotSupportedException()
            };

            // The following is based on the current implementation of System.Formats.Tar
            // We need to prevent attemping to use a disposed stream
            if (tarEntry.DataStream is not null)
            {
                var position = tarEntry.DataStream.Position;
                MemoryStream stream = new((int)tarEntry.Length);

                if (tarEntry.DataStream.CanSeek)
                {
                    tarEntry.DataStream.Position = 0;
                }

                tarEntry.DataStream.CopyTo(stream);

                _ = stream.TryGetBuffer(out var buffer);
                // Preserve the original stream's content and position because setting the clone's stream disposes the original stream
                stream.Position = position;
                tarEntry.DataStream = stream;
                // Clone gets a new stream using the same data buffer
                result.DataStream = new MemoryStream([.. buffer]);
            }

            return result;
        }

        public static System.IO.MemoryMappedFiles.MemoryMappedFile ToMappedFile(this Stream stream)
        {
            var file = System.IO.MemoryMappedFiles.MemoryMappedFile.CreateNew(null, stream.Length);
            using var view = file.CreateViewStream(0, stream.Length, System.IO.MemoryMappedFiles.MemoryMappedFileAccess.Write);
            stream.CopyTo(view);
            view.Flush();
            return file;
        }
    }
}
