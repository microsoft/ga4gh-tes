// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner
{
    internal class MetricsFormatter
    {
        public const string FileSizeToken = "Size";
        public const string TimestampToken = "Time";

        private readonly string _metricsFile;
        private readonly string _fileLogFormat;

        public MetricsFormatter(string metricsFile, string fileLogFormat)
        {
            ArgumentException.ThrowIfNullOrEmpty(metricsFile);
            ArgumentException.ThrowIfNullOrEmpty(fileLogFormat);

            _metricsFile = metricsFile;
            _fileLogFormat = fileLogFormat;
        }

        public Task WriteSize(long bytesTransfered)
        {
            var text = _fileLogFormat.Replace($"{{{FileSizeToken}}}", $"{bytesTransfered:d1}");
            return File.AppendAllTextAsync(_metricsFile, text + Environment.NewLine);
        }

        public Task WriteTime()
        {
            var text = _fileLogFormat.Replace($"{{{TimestampToken}}}", DateTime.UtcNow.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'sszzz", System.Globalization.CultureInfo.InvariantCulture));
            return File.AppendAllTextAsync(_metricsFile, text + Environment.NewLine);
        }

        public async Task WriteWithBash()
        {
            var process = System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo("bash", ["-c", $"({_fileLogFormat}) >> {_metricsFile}"]) { UseShellExecute = true });

            await (process?.WaitForExitAsync() ?? Task.CompletedTask);

            if (process?.ExitCode != 0)
            {
                throw new Exception("Failed to obtain or write metric.");
            }
        }
    }
}
