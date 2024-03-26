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
        /// <summary>
        /// Name of the TES VM performance archive
        /// </summary>
        const string VMPerformanceArchiverFilename = "tes_vm_monitor.tar.gz";

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
        /// Adds lines to download Runner task information file using wget
        /// </summary>
        /// <param name="runnerTaskInfoUrl"></param>
        /// <param name="vmPerfArchiveUrl"></param>
        /// <returns></returns>
        public BatchNodeScriptBuilder WithRunnerTaskDownloadUsingWget(Uri runnerTaskInfoUrl, Uri vmPerfArchiveUrl = null)
        {
            ArgumentNullException.ThrowIfNull(runnerTaskInfoUrl);

            if (useMetricsFile)
            {
                batchScript.AppendLinuxLine("write_ts DownloadRunnerFileStart && \\");
            }

            batchScript.AppendLinuxLine($"{BatchScheduler.CreateWgetDownloadCommand(runnerTaskInfoUrl, $"{BatchTaskDirEnvVar}/{NodeRunnerTaskDefinitionFilename}", setExecutable: false)} && \\");

            if (vmPerfArchiveUrl is not null)
            {
                batchScript.AppendLinuxLine($"{BatchScheduler.CreateWgetDownloadCommand(vmPerfArchiveUrl, $"{BatchTaskDirEnvVar}/{VMPerformanceArchiverFilename}", setExecutable: false)} && \\");
            }

            if (useMetricsFile)
            {
                batchScript.AppendLinuxLine("write_ts DownloadRunnerFileEnd && \\");
            }

            if (vmPerfArchiveUrl is not null)
            {
                // TES vm performance monitoring is bootstrapped and begun by the start_vm_node_monitoring.sh script inside the archive.
                // It is preferable for this to be installed during the start-task. See below for the updated batch_task version if already installed
                //
                // start_vm_node_monitoring.sh will only extract/setup everything in AZ_BATCH_NODE_SHARED_DIR if
                // ${AZ_BATCH_TASK_DIR}/runner-task.json does not exist. Setup will create the {AZ_BATCH_NODE_SHARED_DIR}/vm_monitor/ dir
                // and extracts the contents of the archive into it
                // Note: {AZ_BATCH_NODE_SHARED_DIR}/vm_monitor/start_vm_monitoring.sh is unused. It is easier to call
                //       AZ_BATCH_NODE_SHARED_DIR/start_vm_monitoring.sh
                // When run as normal task and downloaded to $AZ_BATCH_TASK_DIR:
                //      AZ_BATCH_TASK_DIR=/mnt/batch/tasks/workitems/TES_DEBUG_JOB/job-1/debug_task
                //      AZ_BATCH_TASK_DIR=/mnt/batch/tasks/workitems/TES_DEBUG_JOB/job-1/debug_task/wd
                //      AZ_BATCH_NODE_SHARED_DIR=/mnt/batch/tasks/shared
                //batchScript.AppendLinuxLine($"if [ -f \"${{AZ_BATCH_TASK_DIR}}/{VMPerformanceArchiverFilename}\" ]; then");
                //batchScript.AppendLinuxLine($"    tar zxvf \"${{AZ_BATCH_TASK_DIR}}/{VMPerformanceArchiverFilename}\" -C \"${{AZ_BATCH_TASK_DIR}}\" start_vm_node_monitoring.sh");
                //batchScript.AppendLinuxLine("    chmod +x \"${AZ_BATCH_TASK_DIR}/start_vm_node_monitoring.sh\"");
                //batchScript.AppendLinuxLine("    /usr/bin/bash -c \"${AZ_BATCH_TASK_DIR}/start_vm_node_monitoring.sh &\" || true");
                //batchScript.AppendLinuxLine($"fi;");

                // When run as start task and downloaded to $AZ_BATCH_NODE_SHARED_DIR/vm_monitor:
                // mkdir -p $AZ_BATCH_NODE_SHARED_DIR/vm_monitor/
                // wget $url -O $AZ_BATCH_NODE_SHARED_DIR/vm_monitor/tes_vm_monitor.tar.gz
                //      AZ_BATCH_TASK_DIR=/mnt/batch/tasks/startup
                //      AZ_BATCH_TASK_WORKING_DIR=/mnt/batch/tasks/startup/wd
                //      AZ_BATCH_NODE_SHARED_DIR=/mnt/batch/tasks/shared
                // batchScript.AppendLinuxLine($"if [ -f \"${{AZ_BATCH_NODE_SHARED_DIR}}/{VMPerformanceArchiverFilename}\" ]; then");
                // batchScript.AppendLinuxLine($"    tar zxvf \"${{AZ_BATCH_NODE_SHARED_DIR}}/vm_monitor/{VMPerformanceArchiverFilename}\" -C \"${{AZ_BATCH_NODE_SHARED_DIR}}/vm_monitor\" start_vm_node_monitoring.sh");
                // batchScript.AppendLinuxLine("    chmod +x \"${AZ_BATCH_NODE_SHARED_DIR}/vm_monitor/start_vm_node_monitoring.sh\"");
                // batchScript.AppendLinuxLine("    /usr/bin/bash -c \"${AZ_BATCH_NODE_SHARED_DIR}/vm_monitor/start_vm_node_monitoring.sh &\" || true");
                // batchScript.AppendLinuxLine($"fi;");

                // To start the monitor on a task:
                batchScript.AppendLinuxLine(@"if [ -f ""${AZ_BATCH_NODE_SHARED_DIR}/vm_monitor/start_vm_monitoring.sh"" ]; then");
                batchScript.AppendLinuxLine(@"    /usr/bin/bash -c ""${AZ_BATCH_NODE_SHARED_DIR}/vm_monitor/start_vm_node_monitoring.sh &"" || true");
                batchScript.AppendLinuxLine(@"fi;");
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
    }
}
