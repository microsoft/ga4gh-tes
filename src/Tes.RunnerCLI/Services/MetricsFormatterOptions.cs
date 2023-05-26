// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Parsing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tes.RunnerCLI.Commands;

namespace Tes.RunnerCLI.Services
{
    public record MetricsFormatterOptions(string MetricsFile, string FileLogFormat)
    {
        public const string MetricsFormatterOption = "metricsFormatter";

        public static string Description => "Append formatted message to text file. Requires both file and format arguments";

        public static void Configure(Option<MetricsFormatterOptions> option)
        {
            option.AllowMultipleArgumentsPerToken = true;
            option.Arity = new ArgumentArity(2, 2);
            option.ArgumentHelpName = "file> <format"; // This string is already bracketed with '<' & '>' by the help system
        }

        public static MetricsFormatterOptions ParseDownloader(ArgumentResult result)
        {
            return Parse(result, BlobPipelineOptionsConverter.DownloaderFormatterOption);
        }

        public static MetricsFormatterOptions ParseUploader(ArgumentResult result)
        {
            return Parse(result, BlobPipelineOptionsConverter.UploaderFormatterOption);
        }

        private static MetricsFormatterOptions Parse(ArgumentResult result, string optionName)
        {
            if (result.Tokens.Count != 2)
            {
                result.ErrorMessage = $"--{optionName} requires two arguments: file and format";
                return default!;
            }

            return new MetricsFormatterOptions(result.Tokens[0].Value, result.Tokens[1].Value);
        }
    }
}
