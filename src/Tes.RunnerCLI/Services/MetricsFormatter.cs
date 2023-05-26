// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.RunnerCLI.Services
{
    internal class MetricsFormatter
    {
        public const string FileSizeToken = "Size";

        private readonly MetricsFormatterOptions _options;

        public MetricsFormatter(MetricsFormatterOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);

            _options = options;
        }

        public Task Write(long bytesTransfered)
        {
            var text = _options.FileLogFormat.Replace($"{{{FileSizeToken}}}", $"{bytesTransfered:d1}");
            return File.AppendAllTextAsync(_options.MetricsFile, text + Environment.NewLine);
        }
    }
}
