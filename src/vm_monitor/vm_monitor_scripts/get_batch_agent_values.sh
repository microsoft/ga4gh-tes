#!/bin/bash

## Collect optional metrics from the Azure Batch agent, note this is not supported by the Azure Batch service
## values are not guaranteed to be accurate and may change in future versions of the Azure Batch agent
##
## See similar early C# version at:
## https://github.com/gabe-microsoft/CromwellOnAzure/blob/e1537f4f1bd8c0e1789c90e7031e97103eb1d5ca/src/TesApi.Web/BatchScheduler.cs

batchAgentDebugLogPath="/mnt/batch/sys/logs/agent-debug.log"

unix_timestamp_to_iso_time_string() {
    # Convert the numeric unix timestamp to an ISO 8601 timestamp string
    iso_timestamp=$(date -u -d @"$1" +"%Y-%m-%dT%H:%M:%S.%NZ")
    echo "$iso_timestamp"
}

win32_timestamp_to_unix_timestamp() {
    local win32="$1"
    # Subtract the number of seconds between 1601-01-01 and 1970-01-01
    unix=$(echo "scale=9; ($win32 - 116444736000000000) / 10000000" | bc)
    echo "$unix"
}

get_timestamp_or_nan() {
    local timestamp="$1"
    local unix_timestamp
    if [[ -z "$timestamp" ]]; then
        echo "nan"
    else
        unix_timestamp=$(win32_timestamp_to_unix_timestamp "$timestamp")
        unix_timestamp=$(unix_timestamp_to_iso_time_string "$unix_timestamp")
        echo "$unix_timestamp"
    fi
}


get_batch_agent_debug_log_values() {
    # If the batch agent debug log does not exist, return early
    if [[ ! -f "$batchAgentDebugLogPath" ]]; then
        echo "batch_agent_data batch_log_found=\"false\",batch_allocation_time=\"nan\",batch_vm_name=\"\",batch_pool_name=\"\",batch_boot_time=\"nan\""
        return
    fi

    # Extract the logline containing the TVMAllocationTime, PoolName, and TVMName
    # logline=$(sudo grep get_entity_with_sas_async $batchAgentDebugLogPath | grep get_entity_with_sas)
    logline=$(sudo grep TVMAllocationTime $batchAgentDebugLogPath | grep TVMName | grep PoolName | head -1)
    batch_entity_log_line_clean=$(echo "$logline" | python3 clean_log.py)

    # Extract the TVMAllocationTime (this is a win32 timestamp indicating when the VM was allocated by Azure Batch)
    TVMAllocationTime=$(echo "$logline" | grep -oP '"TVMAllocationTime":"\K\d+' | head -1)
    TVMAllocationTime=$(get_timestamp_or_nan "$TVMAllocationTime")

    # Extract the TVMName (the name of the node given by Azure Batch)
    TVMName=$(echo "$logline" | grep -oP '"TVMName":"\K[^"]+' | head -1)
    if [[ -z "$TVMName" ]]; then
        TVMName=""
    fi

    # Extract the PoolName (name of the Batch pool this VM is part of, also available from IMDS)
    PoolName=$(echo "$logline" | grep -oP '"PoolName":"\K[^"]+' | head -1)
    if [[ -z "$PoolName" ]]; then
        PoolName=""
    fi

    # Extract the logline containing the TVMBootTime (note single quotes + space unlike the other params)
    logline=$(sudo grep TVMBootTime $batchAgentDebugLogPath | head -1)
    batch_vmtable_log_line_clean=$(echo "$logline" | python3 clean_log.py)

    TVMBootTime=$(echo "$logline" | grep -oP "'TVMBootTime': '\K\d+")
    TVMBootTime=$(get_timestamp_or_nan "$TVMBootTime")

    echo "batch_agent_data batch_log_found=\"true\",batch_allocation_time=\"$TVMAllocationTime\",batch_vm_name=\"$TVMName\",batch_pool_name=\"$PoolName\",batch_boot_time=\"$TVMBootTime\""
    # Output a copy of the log lines we're pulling from the Batch Agent, these are useful for debugging if this breaks in the future
    echo "debug_batch_agent_entity_log_line data=$batch_entity_log_line_clean"
    echo "debug_batch_agent_vmtable_log_line data=$batch_vmtable_log_line_clean"
}

get_vm_info() {
    # Capture if the system has /sys/firmware/efi, this is a good indicator of UEFI boot (gen2 VMs)
    if [[ -d /sys/firmware/efi ]]; then
        UEFIBoot="true"
    else
        UEFIBoot="false"
    fi
    DMESGOutput=$(dmesg --notime | grep -i "efi: EFI v")

    echo "batch_vm_info vm_has_system_efi=\"$UEFIBoot\",dmesg_efi_version=\"$DMESGOutput\""
}

get_batch_agent_debug_log_values
get_vm_info