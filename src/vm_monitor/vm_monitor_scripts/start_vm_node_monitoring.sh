#!/bin/bash
## This is the script which sets up the Azure Batch TES pefromance monitoring
## environment. This task can be called several times during the lifetime
## of a VM node, for example when the current task ends and new job is picked up
## by the node. It is intended to keep running (in the background) after a TES
## task ends. It will be prempted by the next TES task or the VM being deallocated
##
## Usage: You should run this task in the background as it may need to perform 
## some maintenance/waits before it ends. Outside the Azure Batch env you can
## run this script with the following arguments:
##  $0 <vm_task_name> <working_dir> <task_working_dir> <perf_append_blob_path>
##
## This script is rentrant, and will stop the previous performance montiors and
## start a new one for the current task. Some error conditions on extremely short
## tasks may cause unexpected behavior.
##
## Monitoring via telegraf is run twice, once for one-time metrics and once for 
## continuous monitoring.
##
## TODO: add global task lockfile if needed
## TODO: Optional logging of start_vm_node_monitoring to a file for telegraf to log
## When run as normal task:
##      AZ_BATCH_TASK_DIR=/mnt/batch/tasks/workitems/TES_DEBUG_JOB/job-1/debug_task
##      AZ_BATCH_TASK_DIR=/mnt/batch/tasks/workitems/TES_DEBUG_JOB/job-1/debug_task/wd
##      AZ_BATCH_NODE_SHARED_DIR=/mnt/batch/tasks/shared
## When run as start task:
##      AZ_BATCH_TASK_DIR=/mnt/batch/tasks/startup
##      AZ_BATCH_TASK_WORKING_DIR=/mnt/batch/tasks/startup/wd
##      AZ_BATCH_NODE_SHARED_DIR=/mnt/batch/tasks/shared


get_envs_from_runner_task_json() {
    python3 <<EOF
import json
import urllib.parse
import os

runner_task_file = os.path.join(os.environ['AZ_BATCH_TASK_DIR'], 'runner-task.json')

with open(runner_task_file) as f:
    data = json.load(f)

VM_TASK_NAME = data['Id']
TES_TARGET_URL = data['RuntimeOptions']['StreamingLogPublisher']['TargetUrl']

parsed_url = urllib.parse.urlparse(TES_TARGET_URL)
PERF_STORAGE_ACCOUNT_NAME = parsed_url.netloc.split('.')[0]
PERF_CONTAINER_NAME = parsed_url.path.split('/')[1]

# Get the part of the URL after the container name:
PERF_APPEND_BLOB_PATH = '/'.join(parsed_url.path.split('/')[2:])

print(f'export VM_TASK_NAME="{VM_TASK_NAME}"')
print(f'export PERF_APPEND_BLOB_PATH="{PERF_APPEND_BLOB_PATH}"')
print(f'export PERF_STORAGE_ACCOUNT_NAME="{PERF_STORAGE_ACCOUNT_NAME}"')
print(f'export PERF_CONTAINER_NAME="{PERF_CONTAINER_NAME}"')
EOF
}

SETUP_ONLY=0
if [ $# -gt 4 ]; then
    # Argument handling version for local testing:
    VM_TASK_NAME=$1
    WORKING_DIR=$(dirname "$2")
    TASK_WORKING_DIR=$(dirname "$3")
    PERF_APPEND_BLOB_PATH=$4
    echo "Running in local mode:"
    echo "  VM_TASK_NAME: ${VM_TASK_NAME}"
    echo "  WORKING_DIR: ${WORKING_DIR}"
    echo "  TASK_WORKING_DIR: ${TASK_WORKING_DIR}"
    echo "  PERF_APPEND_BLOB_PATH: ${PERF_APPEND_BLOB_PATH}"
else
    # Get the parameters we need from the environment:
    if [ -z "${AZ_BATCH_NODE_SHARED_DIR}" ] || [ -z "${AZ_BATCH_TASK_DIR}" ] || [ -z "${AZ_BATCH_TASK_WORKING_DIR}" ]; then
        echo "This script is intended to be run in an Azure Batch task, expected AZ_BATCH variables not found."
        echo "Usage: $0 <vm_task_name> <working_dir> <task_working_dir> <perf_append_blob_path>"
        echo -e "\n\nExiting start_vm_node_monitoring.sh"
        exit 1
    fi
    echo "Running in Azure Batch task mode"
    WORKING_DIR=${AZ_BATCH_NODE_SHARED_DIR}/vm_monitor
    TASK_WORKING_DIR=${AZ_BATCH_TASK_WORKING_DIR}
    if [[ -f "${AZ_BATCH_TASK_DIR}/runner-task.json" ]]; then
        eval "$(get_envs_from_runner_task_json)"
    else
        echo "  runner-task.json not found, performing extraction/setup only"
        SETUP_ONLY=1
    fi

    # Add a trailing slash to PERF_APPEND_BLOB_PATH if it doesn't have one:
    if [[ ! ${PERF_APPEND_BLOB_PATH} =~ /$ ]]; then
        PERF_APPEND_BLOB_PATH="${PERF_APPEND_BLOB_PATH}/"
    fi
fi
# WORKING_DIR should not end in a slash:
WORKING_DIR=${WORKING_DIR%/}

# If start_vm_monitoring.sh is not in ${WORKING_DIR} then note it and update the WORKING_DIR
BASH_SCRIPT_PATH=$(dirname "$(readlink -f "$0")")
if [ "$BASH_SCRIPT_PATH" != "${WORKING_DIR}" ]; then
    echo "start_vm_monitoring.sh is not in the expected directory, updating WORKING_DIR to: $BASH_SCRIPT_PATH"
    WORKING_DIR=$BASH_SCRIPT_PATH
fi

PERF_ARCHIVE_FILENAME="${WORKING_DIR}/tes_vm_monitor.tar.gz"
PERF_SCRIPT_DIR="${WORKING_DIR}/scripts"
ONE_TIME_CONFIG="${PERF_SCRIPT_DIR}/tes_vm_monitor.once.conf"
CONTINUOUS_CONFIG="${PERF_SCRIPT_DIR}/tes_vm_monitor.continuous.conf"
ONE_TIME_CONFIG_CLEAN="${ONE_TIME_CONFIG%.conf}.clean.conf"
CONTINUOUS_CONFIG_CLEAN="${CONTINUOUS_CONFIG%.conf}.clean.conf"

function extract_resources() {
    # If the data is already extracted, do nothing:
    if [ -d "${PERF_SCRIPT_DIR}" ]; then
        echo "Skipping extraction ${PERF_SCRIPT_DIR} already exists."
        return
    fi
    # If the archive is not present, do nothing:
    if [ ! -f "${PERF_ARCHIVE_FILENAME}" ]; then
        echo "Skipping extraction ${PERF_ARCHIVE_FILENAME} does not exist."
        return
    fi

    # Extract telegraf and all the scripts:
    mkdir -p "${PERF_SCRIPT_DIR}"
    tar zxvf "${PERF_ARCHIVE_FILENAME}" -C "${PERF_SCRIPT_DIR}/"
    chmod a+x "${PERF_SCRIPT_DIR}/telegraf"
    chmod a+x "${PERF_SCRIPT_DIR}/run_telegraf.sh"
    chmod a+x "${PERF_SCRIPT_DIR}"/*.sh

    # Create the telegraf compact/cleaned config files:
    create_telegraf_configs
}

# Create 'clean' versions of the telegraf input configs for running telegraf and logging
# This will remove all lines starting with whitespace and a '#' and all blank lines
# Inline comments are left alone
function create_telegraf_configs() {
    grep -vE '^\s*#' "${ONE_TIME_CONFIG}" | grep -vE '^\s*$' > "${ONE_TIME_CONFIG_CLEAN}"
    grep -vE '^\s*#' "${CONTINUOUS_CONFIG}" | grep -vE '^\s*$' > "${CONTINUOUS_CONFIG_CLEAN}"
}

# Screen returns a list of all screen sessions, we filter for sessions of the form:
# {screen_pid}.TELEGRAF__{VM_TASK_NAME}
# We ignore the current screen sessions and send a ctrl-C or passed in signal to the rest
function stop_other_telegraf_screens() {
    local CURRENT_SCREEN_NAME=${1}
    local SIGNAL=${2}
    # End existing 'TELEGRAF__*' screen sessions:
    for session in $(screen -ls | grep -o '[0-9]*\.TELEGRAF__\S*' | grep -v "${CURRENT_SCREEN_NAME}"); do
        # If no signal is given send a Ctrl-C to the session:
        if [ -z "${SIGNAL}" ]; then
            echo "Sending ctrl-c to existing telegraf session: ${session}"
            screen -S "${session}" -X stuff "^C"
        else
            echo "Quitting existing telegraf session: ${session}"
            # shellcheck disable=SC2086
            screen -S "${session}" -X ${SIGNAL}
        fi
    done
}

# If we find any screens running which are not the current screen, return 0
function check_for_other_telegraf_screens() {
    local CURRENT_SCREEN_NAME=${1}
    for session in $(screen -ls | grep -o '[0-9]*\.TELEGRAF__\S*' | grep -v "${CURRENT_SCREEN_NAME}"); do
        return 0
    done
    return 1
}

# Handle the case where there is more than one CURRENT_SCREEN_NAME session
# we keep the last session to run and kill the rest
# Not to be confused with the code later on which asks screens running with different task names to exit
function keep_latest_telegraf_screen() {
    local SCREEN_NAME=${1}
    local SESSIONS
    local SESSION_COUNT
    local PID
    # Get a list of all screen sessions with the given name, sorted by creation time:
    SESSIONS=$(screen -ls | grep "${SCREEN_NAME}" | awk '{print $1}' | sort -n)
    SESSION_COUNT=$(echo "${SESSIONS}" | wc -l)
    echo -e "\n\nWARNING: Found ${SESSION_COUNT} telegraf sessions with the name: ${SCREEN_NAME}"
    # If there is more than one session with the given name:
    if [ "${SESSION_COUNT}" -gt 1 ]; then
        KEPT_SESSION=$(echo "${SESSIONS}" | tail -n 1)
        echo "Keeping the latest telegraf session: ${KEPT_SESSION}"
        # Remove the last session from the list (the latest one):
        SESSIONS=$(echo "${SESSIONS}" | head -n -1)

        # Ask session to gracefully quit:
        for session in ${SESSIONS}; do
            echo "Sending ctrl-c to existing telegraf session: ${session}"
            screen -S "${session}" -X stuff "^C"
        done
        echo "Waiting 5s before checking if any other sessions remain"
        # Pause before checking if any other sessions remain (this for loop is a fancy sleep 5 that can return early)
        # shellcheck disable=SC2034
        for i in {1..10}; do
            # Check if any sessions are still running
            if ! screen -list | grep -q "${session}"; then
                break # If no sessions are running, break the loop
            fi
            sleep 0.5  # If sessions are still running, sleep for 0.5 seconds
        done

        # Kill all remaining sessions (the older ones):
        for session in ${SESSIONS}; do
            # Check if this session is still running:
            if screen -list | grep -q "${session}"; then
                echo "Killing old telegraf session: ${session}"
                # Get the PID of the screen session:
                PID=$(screen -list | grep "${session}" | cut -d'.' -f1)
                # Kill the screen session and all its child processes (SIGTERM):
                pkill -TERM -P "${PID}"
            fi
        done
    fi
}

function terminate_other_telegraf_screns(){
    local CURRENT_SCREEN_NAME=${1}
    local PID
    # End existing 'TELEGRAF__*' screen sessions:
    for session in $(screen -ls | grep -o '[0-9]*\.TELEGRAF__\S*' | grep -v "${CURRENT_SCREEN_NAME}"); do
        echo "Killing existing telegraf session: ${session}"
        # Get the PID of the screen session:
        PID=$(screen -list | grep "${session}" | cut -d'.' -f1)
        # Kill the screen session and all its child processes (SIGKILL):
        pkill -9 -P "${PID}"
    done
}
# Extract resources only if we need to and setup the telegraf configs:
extract_resources
if [ $SETUP_ONLY -eq 1 ]; then
    echo "Exiting start_vm_node_monitoring.sh, setup complete"
    exit 0
fi

# Write the environmental variables to a file for the run_telegraf.sh script to import:
TASK_ENV_FILE="${PERF_SCRIPT_DIR}/tmp_task_env__${VM_TASK_NAME}.sh"
rm -f "${TASK_ENV_FILE}"
{
    echo "export VM_TASK_NAME=\"${VM_TASK_NAME}\""
    echo "export TASK_WORKING_DIR=\"${TASK_WORKING_DIR}\""
    echo "export PERF_SCRIPT_DIR=\"${PERF_SCRIPT_DIR}\""
    echo "export ONE_TIME_CONFIG_CLEAN=\"${ONE_TIME_CONFIG_CLEAN}\""
    echo "export CONTINUOUS_CONFIG_CLEAN=\"${CONTINUOUS_CONFIG_CLEAN}\""
    echo "export PERF_STORAGE_ACCOUNT_NAME=\"${PERF_STORAGE_ACCOUNT_NAME}\""
    echo "export PERF_CONTAINER_NAME=\"${PERF_CONTAINER_NAME}\""
    echo "export PERF_APPEND_BLOB_PATH=\"${PERF_APPEND_BLOB_PATH}\""
} > "${TASK_ENV_FILE}"

# Start the current monitor then stop the others:
SCREEN_NAME="TELEGRAF__${VM_TASK_NAME}"
# Start a new screen session with the current VM_TASK_NAME:
screen -S "${SCREEN_NAME}" -dm bash -c "${PERF_SCRIPT_DIR}/run_telegraf.sh \"${TASK_ENV_FILE}\""
echo "Started telegraf for task \"${VM_TASK_NAME}\" in screen session \"${SCREEN_NAME}\""

# Make sure there is only one telegraf session running for the curretn VM_TASK_NAME
keep_latest_telegraf_screen "${SCREEN_NAME}"

# Gracefully end existing 'TELEGRAF__*' screen sessions (SIGINT) with other VM_TASK_NAMEs
if check_for_other_telegraf_screens "${SCREEN_NAME}"; then
    echo -e "\n\nStopping other telegraf sessions"
    stop_other_telegraf_screens "${SCREEN_NAME}"
    echo "  Waiting 5s before checking if any other sessions remain"
    sleep 5 # Pause before checking if any other sessions remain
fi
# Wait 30s then kill any other telegraf sessions:
if check_for_other_telegraf_screens "${SCREEN_NAME}"; then
    echo -e "\n\nWaiting to quit other telegraf sessions"
    sleep 30
    # Quit any remaining 'TELEGRAF__*' screen sessions (SIGTERM) with other VM_TASK_NAMEs
    stop_other_telegraf_screens "${SCREEN_NAME}" "quit"
    echo "  Waiting 5s before checking if any other sessions remain"
    sleep 5 # Pause before checking if any other sessions remain
fi
if check_for_other_telegraf_screens "${SCREEN_NAME}"; then
    echo -e "\n\nWaiting to kill other telegraf sessions"
    sleep 5
    # Kill any remaining 'TELEGRAF__*' screen sessions (SIGKILL), SIGKILL should 
    # only ever be necessary in cases of a serious bug:
    terminate_other_telegraf_screns "${SCREEN_NAME}"
fi
echo -e "\n\nFinished starting telegraf for task \"${VM_TASK_NAME}\""
echo "Exiting start_vm_node_monitoring.sh"

exit 0
