#!/usr/bin/bash
trap "echo Error trapped; exit 0" ERR
# set -e will cause any error to exit the script
set -e

if [ -f "${AZ_BATCH_NODE_SHARED_DIR}/{VMPerformanceArchiverFilename}" ]; then
    tar zxvf "${AZ_BATCH_NODE_SHARED_DIR}/vm_monitor/{VMPerformanceArchiverFilename}" -C "${AZ_BATCH_NODE_SHARED_DIR}/vm_monitor" start_vm_node_monitoring.sh
    chmod +x "${AZ_BATCH_NODE_SHARED_DIR}/vm_monitor/start_vm_node_monitoring.sh"
    /usr/bin/bash -c "${AZ_BATCH_NODE_SHARED_DIR}/vm_monitor/start_vm_node_monitoring.sh &" || true
fi;
