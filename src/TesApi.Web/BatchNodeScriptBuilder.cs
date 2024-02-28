// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Text;
using System.Threading;
using Tes.Models;
using TesApi.Web.Extensions;
using TesApi.Web.Runner;

namespace TesApi.Web
{
    /// <summary>
    /// Builder of batch command script to be executed on a Batch node. 
    /// </summary>
    public class BatchNodeScriptBuilder
    {
        /// <summary>
        /// Name of the environment variable that contains the path to the task directory
        /// </summary>
        public const string BatchTaskDirEnvVarName = "AZ_BATCH_TASK_DIR";

        private const string BatchTaskDirEnvVar = $"${BatchTaskDirEnvVarName}";

        /// <summary>
        /// Name of the TES runner binary file
        /// </summary>
        public const string NodeTaskRunnerFilename = "tes-runner";
        /// <summary>
        /// Name of the TES runner task definition file
        /// </summary>
        public const string NodeRunnerTaskDefinitionFilename = "runner-task.json";

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
            batchScript.AppendLinuxLine($"write_kv() {{ echo \"$1=$2\" >> {BatchTaskDirEnvVar}/{TaskToNodeTaskConverter.MetricsFileName}; }} && \\");  // Function that appends key=value pair to metrics.txt file
            batchScript.AppendLinuxLine($"write_ts() {{ write_kv $1 $(date -Iseconds); }} && \\");    // Function that appends key=<current datetime> to metrics.txt file

            return this;
        }

        /// <summary>
        /// Adds lines to download Runner binary and its task information file using wget
        /// </summary>
        /// <param name="runnerBinaryUrl"></param>
        /// <param name="runnerTaskInfoUrl"></param>
        /// <returns></returns>
        public BatchNodeScriptBuilder WithRunnerFilesDownloadUsingWget(Uri runnerBinaryUrl, Uri runnerTaskInfoUrl)
        {
            return WithRunnerFilesDownloadUsingWget(runnerBinaryUrl, runnerTaskInfoUrl, null);
        }

        /// <summary>
        /// Adds lines to download Runner binary and its task information file using wget
        /// </summary>
        /// <param name="runnerBinaryUrl"></param>
        /// <param name="runnerTaskInfoUrl"></param>
        /// <param name="vmPerfArchiveUrl"></param>
        /// <returns></returns>
        public BatchNodeScriptBuilder WithRunnerFilesDownloadUsingWget(Uri runnerBinaryUrl, Uri runnerTaskInfoUrl, Uri? vmPerfArchiveUrl)
        {
            ArgumentNullException.ThrowIfNull(runnerBinaryUrl);
            ArgumentNullException.ThrowIfNull(runnerTaskInfoUrl);

            const string VMPerformanceArchiverFilename = "tes_vm_perf.tar.gz";

            if (useMetricsFile)
            {
                batchScript.AppendLinuxLine("write_ts DownloadRunnerFileStart && \\");
            }

            batchScript.AppendLinuxLine($"{CreateWgetDownloadCommand(runnerBinaryUrl, $"{BatchTaskDirEnvVar}/{NodeTaskRunnerFilename}", setExecutable: true)} && \\");
            batchScript.AppendLinuxLine($"{CreateWgetDownloadCommand(runnerTaskInfoUrl, $"{BatchTaskDirEnvVar}/{NodeRunnerTaskDefinitionFilename}", setExecutable: false)} && \\");

            if (vmPerfArchiveUrl != null)
            {
                batchScript.AppendLinuxLine($"{CreateWgetDownloadCommand(vmPerfArchiveUrl, $"{BatchTaskDirEnvVar}/{VMPerformanceArchiverFilename}", setExecutable: false)} && \\");
            }
            
            if (useMetricsFile)
            {
                batchScript.AppendLinuxLine("write_ts DownloadRunnerFileEnd && \\");
            }

            if (vmPerfArchiveUrl != null)
            {
                // TES vm performance monitoring is bootstrapped and begun by the script inside the archive:
                batchScript.AppendLinuxLine($"tar zxvf {VMPerformanceArchiverFilename} -C \"${{AZ_BATCH_TASK_DIR}}\" start_vm_node_monitoring.sh && \\");
                batchScript.AppendLinuxLine("chmod +x \"${AZ_BATCH_TASK_DIR}/start_vm_node_monitoring.sh\" && \\");
                batchScript.AppendLinuxLine("/usr/bin/bash -c \"${AZ_BATCH_TASK_DIR}/start_vm_node_monitoring.sh &\" && \\");
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

            batchScript.AppendLinuxLine($"{BatchTaskDirEnvVar}/{NodeTaskRunnerFilename} -f {BatchTaskDirEnvVar}/{NodeRunnerTaskDefinitionFilename} && \\");

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
                batchScript.AppendLinuxLine($"/bin/bash -c 'disk=( `df -k {BatchTaskDirEnvVar} | tail -1` ) && echo DiskSizeInKiB=${{disk[1]}} >> {BatchTaskDirEnvVar}/{TaskToNodeTaskConverter.MetricsFileName} && echo DiskUsedInKiB=${{disk[2]}} >> {BatchTaskDirEnvVar}/{TaskToNodeTaskConverter.MetricsFileName}' && \\");
                batchScript.AppendLinuxLine($"write_kv VmCpuModelName \"$(cat /proc/cpuinfo | grep -m1 name | cut -f 2 -d ':' | xargs)\" && \\");
            }

            return this;
        }

        /// <summary>
        /// Builds the Batch command script
        /// </summary>
        /// <returns></returns>
        public virtual string Build()
        {
            batchScript.AppendLinuxLine("echo Task complete");

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
        public static string CreateWgetDownloadCommand(Uri urlToDownload, string localFilePathDownloadLocation, bool setExecutable = false)
        {
            ArgumentNullException.ThrowIfNull(urlToDownload);
            ArgumentException.ThrowIfNullOrEmpty(localFilePathDownloadLocation);

            var command = $"wget --https-only --no-verbose --timeout=20 --waitretry=1 --tries=9 --retry-connrefused --continue -O {localFilePathDownloadLocation} '{urlToDownload.AbsoluteUri}'";

            if (setExecutable)
            {
                command += $" && chmod +x {localFilePathDownloadLocation}";
            }

            return command;
        }
    }
}
