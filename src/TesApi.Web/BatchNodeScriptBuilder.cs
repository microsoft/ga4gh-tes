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

        private readonly StringBuilder batchScript = new();
        private bool useMetricsFile;

        /// <summary>
        /// Constructor of BatchCommandBuilder
        /// </summary>
        public BatchNodeScriptBuilder()
        {
            batchScript.AppendLinuxLine("#!/usr/bin/bash");
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
        /// Adds lines to download Runner task information file using wget
        /// </summary>
        /// <param name="runnerTaskInfoUrl"></param>
        /// <returns></returns>
        public BatchNodeScriptBuilder WithRunnerTaskDownloadUsingWget(Uri runnerTaskInfoUrl)
        {
            ArgumentNullException.ThrowIfNull(runnerTaskInfoUrl);

            if (useMetricsFile)
            {
                batchScript.AppendLinuxLine("write_ts DownloadRunnerFileStart && \\");
            }

            batchScript.AppendLinuxLine($"{BatchScheduler.CreateWgetDownloadCommand(runnerTaskInfoUrl, $"{BatchTaskDirEnvVar}/{NodeRunnerTaskDefinitionFilename}", setExecutable: false)} && \\");

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

            batchScript.AppendLinuxLine($"{BatchScheduler.BatchNodeSharedEnvVar}/{NodeTaskRunnerFilename} -f {BatchTaskDirEnvVar}/{NodeRunnerTaskDefinitionFilename} && \\");

            if (useMetricsFile)
            {
                batchScript.AppendLinuxLine("write_ts ExecuteNodeTesTaskEnd && \\");
            }

            return this;
        }

        /// <summary>
        /// Adds line that executes the runner
        /// </summary>
        /// <returns></returns>
        public BatchNodeScriptBuilder WithExecuteRunner(Uri nodeTask)
        {
            if (useMetricsFile)
            {
                batchScript.AppendLinuxLine("write_ts ExecuteNodeTesTaskStart && \\");
            }

            batchScript.AppendLinuxLine($"{BatchScheduler.BatchNodeSharedEnvVar}/{NodeTaskRunnerFilename} -i '{(new Azure.Storage.Blobs.BlobUriBuilder(nodeTask) { Sas = null }).ToUri().AbsoluteUri}' && \\");

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
                batchScript.AppendLinuxLine($"disk=( `df -k {BatchTaskDirEnvVar} | tail -1` ) && echo DiskSizeInKiB=${{disk[1]}} >> {BatchTaskDirEnvVar}/{TaskToNodeTaskConverter.MetricsFileName} && echo DiskUsedInKiB=${{disk[2]}} >> {BatchTaskDirEnvVar}/{TaskToNodeTaskConverter.MetricsFileName} && \\");
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
    }
}
