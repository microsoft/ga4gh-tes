// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Text;
using TesApi.Web.Extensions;
using TesApi.Web.Runner;

namespace TesApi.Web
{
    /// <summary>
    /// Builder of batch command script to be executed on a Batch node. 
    /// </summary>
    public class BatchNodeScriptBuilder
    {
        private const string NodeTaskRunnerFilename = "tRunner";
        private const string NodeRunnerTaskInfoFilename = "TesTask.json";
        private const string AzBatchTaskDirEnvVar = "$AZ_BATCH_TASK_DIR";
        private readonly StringBuilder batchScript;
        private bool useMetricsFile;

        /// <summary>
        /// Constructor of BatchCommandBuilder
        /// </summary>
        public BatchNodeScriptBuilder()
        {
            batchScript = new StringBuilder();
        }

        /// <summary>
        /// Adds line that installs wget if running on alpine
        /// </summary>
        /// <returns></returns>
        public BatchNodeScriptBuilder WithAlpineWgetInstallation()
        {
            batchScript.AppendLinuxLine($"(grep -q alpine /etc/os-release && apk add bash wget || :) && \\");  // Install bash if running on alpine (will be the case if running inside "docker" image)

            return this;
        }

        /// <summary>
        /// Adds lines to create the metrics file and prepare bash function to append data to it. 
        /// </summary>
        /// <returns></returns>
        public BatchNodeScriptBuilder WithMetrics()
        {
            useMetricsFile = true;
            batchScript.AppendLinuxLine($"write_kv() {{ echo \"$1=$2\" >> {AzBatchTaskDirEnvVar}/{TesTaskToNodeTaskConverter.MetricsFileName}; }} && \\");  // Function that appends key=value pair to metrics.txt file
            batchScript.AppendLinuxLine($"write_ts() {{ write_kv $1 $(date -Iseconds); }} && \\");    // Function that appends key=<current datetime> to metrics.txt file

            return this;
        }

        /// <summary>
        /// Adds lines to download Runner binary and its task information file using wget
        /// </summary>
        /// <param name="runnerBinaryUrl"></param>
        /// <param name="runnerTaskInfoUrl"></param>
        /// <returns></returns>
        public BatchNodeScriptBuilder WithRunnerFilesDownloadUsingWget(string runnerBinaryUrl, string runnerTaskInfoUrl)
        {
            ArgumentException.ThrowIfNullOrEmpty(runnerBinaryUrl, nameof(runnerBinaryUrl));
            ArgumentException.ThrowIfNullOrEmpty(runnerTaskInfoUrl, nameof(runnerTaskInfoUrl));

            if (useMetricsFile)
            {
                batchScript.AppendLinuxLine("write_ts DownloadRunnerFileStart && \\");
            }

            batchScript.AppendLinuxLine(CreateWgetDownloadCommand(runnerBinaryUrl, $"{AzBatchTaskDirEnvVar}/{NodeTaskRunnerFilename}", setExecutable: true));
            batchScript.AppendLinuxLine(CreateWgetDownloadCommand(runnerTaskInfoUrl, $"{AzBatchTaskDirEnvVar}/{NodeRunnerTaskInfoFilename}", setExecutable: false));

            if (useMetricsFile)
            {
                batchScript.AppendLinuxLine("write_ts DownloadRunnerFileEnd && \\");
            }

            return this;
        }

        /// <summary>
        /// Adds line that executes the runner
        /// </summary>
        /// <returns></returns>
        public BatchNodeScriptBuilder WithExecuteRunner()
        {
            if (useMetricsFile)
            {
                batchScript.AppendLinuxLine("write_ts ExecuteNodeTesTaskStart && \\");
            }

            batchScript.AppendLinuxLine($"({AzBatchTaskDirEnvVar}/{NodeTaskRunnerFilename} || :) && \\");

            if (useMetricsFile)
            {
                batchScript.AppendLinuxLine("write_ts ExecuteNodeTesTaskEnd && \\");
            }

            return this;
        }

        /// <summary>
        /// Adds local runtime system information to the metrics file.
        /// </summary>
        /// <returns></returns>
        public BatchNodeScriptBuilder WithLocalRuntimeSystemInformation()
        {
            if (useMetricsFile)
            {
                batchScript.AppendLinuxLine($"/bin/bash -c 'disk=( `df -k {AzBatchTaskDirEnvVar} | tail -1` ) && echo DiskSizeInKiB=${{disk[1]}} >> {AzBatchTaskDirEnvVar}/{TesTaskToNodeTaskConverter.MetricsFileName} && echo DiskUsedInKiB=${{disk[2]}} >> {AzBatchTaskDirEnvVar}/{TesTaskToNodeTaskConverter.MetricsFileName}' && \\");
                batchScript.AppendLinuxLine($"write_kv VmCpuModelName \"$(cat /proc/cpuinfo | grep -m1 name | cut -f 2 -d ':' | xargs)\" && \\");
            }

            return this;
        }

        /// <summary>
        /// Builds the Batch command script
        /// </summary>
        /// <returns></returns>
        public string Build()
        {
            var builtScript = batchScript.ToString();
            batchScript.Clear();

            return builtScript;
        }

        /// <summary>
        /// Creates a wget command to robustly download a file
        /// </summary>
        /// <param name="urlToDownload">URL to download</param>
        /// <param name="localFilePathDownloadLocation">Filename for the output file</param>
        /// <param name="setExecutable">Whether the file should be made executable or not</param>
        /// <returns>The command to execute</returns>
        private string CreateWgetDownloadCommand(string urlToDownload, string localFilePathDownloadLocation, bool setExecutable = false)
        {
            string command = $"wget --https-only --timeout=20 --waitretry=1 --tries=9 --retry-connrefused --continue -O {localFilePathDownloadLocation} '{urlToDownload}'";

            if (setExecutable)
            {
                command += $" && chmod +x {localFilePathDownloadLocation}";
            }

            return command;
        }
    }
}
