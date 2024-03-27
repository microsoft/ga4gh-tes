# TES Performance Metrics Data Collection

Performance stats are collected on a per Azure Virtual Machine basis using the Telegraf collection agent. Data are written to the `<StorageAccount/<ContainerName>/<RootWorkflow>/<TaskName>/<ExecutionAttempt>` directory as an Azure Append Blob. On initialization a snapshot of the system is captured and flushed to the Append Blob, there after performance metrics are aggregated and flushed every 2 minutes (set by `Agent.flush_interval`).

Telegraf provides a set of input, output, and aggregator plugins. To limit the binary size for Telegraf we custom compile and strip a Telegraf binary going from ~220MB to ~21MB but having only the plugins we explicitly use available.

Azure Append Blob output plugins are provided by a custom Go Telegraf plugin `azure_append_blob` with some TES specific logic (i.e., this is not intended to be a generic Append Blob plugin)

Additional one-time stats are collected using Telegraf's shell `exec` plugin and a set of bash and Python3 scripts to extract information from:

* `/proc/cpuinfo`
* `lscpu`
* system boot time (with sub second accuracy)
* unsupported (hacked) metrics from the Azure Batch Agent logs
* Azure Instance Metadata Service (IMDS)
* `nvme` block devices

## Telegraf custom binary building

Use the `build_tes_deployment_archive.sh` script to build a locally setup telegraf repo. Or call `telegraf/build_telegraf_remote.sh` to build on a remote Azure Batch VM. Otherwise you can follow the steps below:

1. Git clone Telegraf repo
1. Apply patches to add azure_append_blob
1. `make build_tools` to build the Telegraf custom_builder
1. Have Telegraf read our config file to build only the needed plugins. We export a LDFLAG to strip and remove debug information from the resulting static Go binary. This saves ~50% of the executable size. `export LDFLAGS="-s -w"; ./tools/custom_builder/custom_builder --config ./azure_append_test.config`

## Configuring Telegraf dynamically

Some plugins like the AMD ROCm GPU or Nvidia-SMI GPU input plugin have a 'startup_error_behavior' configuration option. When set to `ignore` the plugin won't capture data if the VM doesn't support that input (e.g. it doesn't have a GPU).

The `start_vm_node_monitoring_.sh` script can be modified to make changes to the Telegraf config file. For example, `infiniband` plugins should only be enabled on InfiniBand equipped machines.

Note that the `file` input is used to read a comment striped version of the config file into the output log once.

## Upload .tar.gz data to tes-internal

Use `build_tes_deployment_archive.sh` to collect the scripts and telegraf binary into a single gziped binary. Then upload this binary to the `tes-internal` container. A modification to the `batch_script` used to run each task will need to be made to download and start data collection.

## Collection process

On download the `start_vm_node_monitoring.sh` script bootstraps the Telegraf logging process. The intent is that a node which gets multiple tasks will gracefully move from one monitoring session to another. That is, if you have a monitor running for the current task it will keep going after the current task ends. When a new task is picked up it, the new task will start a new logging session and ask (with increasing insistence) the previous logger to end.

If the node terminates, telegraf will attempt to flush data but will most likely fail. This means node runtime roughly is within 2 minutes (`Agent.flush_interval` + append blob upload time) of the actual node runtime.

Azure VMs typically are shutdown with ACPI power off style signal, so there is no time to respond or flush the final set of outputs. IMDS Scheduled Events do not help with deallocation signaling. Azure Batch machines power off immediately before the guest has time to receive a Scheduled Event.

## What gets run on a Batch Node?

On the first task run the `<cromwell_storage_account>/cromwell-executions/.../tes_task/batch_script` will contain code to download the `tes_vm_monitor.tar.gz` archive built by `build_tes_deploymnet_archive.sh`. Next it will extract, chmod, and run in the background the `start_vm_node_monitoring.sh` script.

### `start_vm_node_monitoring.sh`

This script performs two main tasks. It is reentrant provided you have a new `TES_TASK_NAME`. Otherwise you can have multiple screens running with the same `TES_TASK_NAME`. The function `keep_latest_telegraf_screen` will attempt to ask other sessions to end and then kill them.

1. Extracts the `tes_vm_monitor.tar.gz` archive into `${AZ_BATCH_TASK_DIR}/tes_vm_monitor/` (typically `/mnt/cromwell/tes_vm_monitor`). It makes sure the telegraf binary is executable, along with all the .sh scripts. It will also take the .conf files and create cleaned versions of them (with all comments removed) to use for logging. These ``.clean.conf` are written to the Telegraf output.

2. Prepares a running environment for the current Telegraf monitoring instance (i.e. serializing variables for the launcher `tes_vm_monitor/run_telegraf.sh` to load). Launches the current monitor. And then looks to see if any other monitors are running that should be terminated.

    1. For other sessions that have the same `TES_TASK_NAME` it will send them a `ctrl-c` command to gracefully flush and exit. Wait 10s for the flush to happen, and then kill the other processes.

    1. For sessions that start with `TELEGRAF__` that aren't for the same `TES_TASK_NAME` it sends `ctrl-c` to ask the program to gracefully flush and exit. Waits ~30s. Then asks the screen to `quit` and also gracefully exit. Waits another 5s. Then it terminates using `SIGKILL` the other screens processes.

### `run_telegraf.sh`

This script launches two instances of Telegraf one after another. First it launches the `${ONE_TIME_CONFIG_CLEAN}` version of the config which should include some one-time system monitoring (i.e., running `collect_tes_perf.sh` which should run the other IMDS, nvme, lscpu, /proc/cpu, sub-second boot time, etc. scripts).

Then it launches a version of `${CONTINUOUS_CONFIG_CLEAN}` which should be the long running performance monitoring session. `run_telegraf.sh` will attempt to restart telegraf if it dies. There is a `check_iteration_rate` function that does a simple 5 minute sleep if telegraf keeps on dying.
