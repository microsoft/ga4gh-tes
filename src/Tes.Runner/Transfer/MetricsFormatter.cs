// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner
{
    internal class MetricsFormatter
    {
        public const string FileSizeToken = "Size";

        private readonly string _metricsFile;
        private readonly string _fileLogFormat;

        public MetricsFormatter(string metricsFile, string fileLogFormat)
        {
            ArgumentException.ThrowIfNullOrEmpty(metricsFile);
            ArgumentException.ThrowIfNullOrEmpty(fileLogFormat);

            _metricsFile = metricsFile;
            _fileLogFormat = fileLogFormat;
        }

        public Task Write(long bytesTransfered)
        {
            var text = _fileLogFormat.Replace($"{{{FileSizeToken}}}", $"{bytesTransfered:d1}");
            return File.AppendAllTextAsync(_metricsFile, text + Environment.NewLine);
        }
    }
}
