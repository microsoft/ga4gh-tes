#!/bin/bash

## This script is run in a screen session. If the telegraf process dies, the process will be restarted.
## However if the screen session quits, or ctrl-c is sent to the sesssion, the script will end.

function run_telegraf_once() {
    ./telegraf --config "${ONE_TIME_CONFIG_CLEAN}" --once --debug
}

function run_telegraf_continuous() {
    ./telegraf --config "${CONTINUOUS_CONFIG_CLEAN}" --debug
}

# Source the environment variables from the file specified in the first argument
if [ -z "$1" ]; then
    echo "Warning: run_telegraf.sh should be called with an argument specifying the environment file to source."
fi
ENV_FILE="$1"
# shellcheck disable=SC1090
source "$ENV_FILE"

echo "Starting telegraf..."
echo "Environment information:"
echo "  PERF_SCRIPT_DIR: ${PERF_SCRIPT_DIR}"
echo "  ONE_TIME_CONFIG_CLEAN: ${ONE_TIME_CONFIG_CLEAN}"
echo "  CONTINUOUS_CONFIG_CLEAN: ${CONTINUOUS_CONFIG_CLEAN}"
echo "  PERF_APPEND_BLOB_PATH: ${PERF_APPEND_BLOB_PATH}"
echo "  PERF_STORAGE_ACCOUNT_NAME: ${PERF_STORAGE_ACCOUNT_NAME}"
echo "  PERF_CONTAINER_NAME: ${PERF_CONTAINER_NAME}"
echo "  TASK_WORKING_DIR: ${TASK_WORKING_DIR}"
echo -e "\n\nRunning telegraf once..."

cd "${PERF_SCRIPT_DIR}" || exit 1

# Remove the context boostrap file
if [[ -f "$ENV_FILE" && "$(basename "$ENV_FILE")" == tmp_* ]]; then
    rm -f "$ENV_FILE"
fi

# One time initial run with an immediately flush to append blob:
run_telegraf_once

echo -e "\n\n\n\n\nRunning telegraf continuously..."

# Monitor SIGINT (Ctrl-C) to turn off the loop and pass ctrl-c to the currently running telegraf instance
export TELEGRAF_PID=""
trap 'kill -SIGINT $TELEGRAF_PID; LOOP=0' SIGINT
# Monitor SIGTERM to turn off the loop, these are terminal signals (SIGKILL and SIGSTOP cannot be caught)
trap 'kill -SIGTERM $TELEGRAF_PID; LOOP=0' SIGTERM
trap 'kill -SIGHUP $TELEGRAF_PID; LOOP=0' SIGHUP

# Function to limit telegraf restart rate (to keep resource usage down)
# If telegraf dies twice in 30s, we will sleep for 5 minutes before attempting to restart it
export ITERATION_COUNT=0
PREV_TIMESTAMP=$(date +%s)
export PREV_TIMESTAMP
export ITERATION_LIMIT=2
function check_iteration_rate() {
    local CURRENT_TIMESTAMP
    CURRENT_TIMESTAMP=$(date +%s)
    if [ $((CURRENT_TIMESTAMP - PREV_TIMESTAMP)) -le 30 ]; then
        ITERATION_COUNT=$((ITERATION_COUNT + 1))
        if [ $ITERATION_COUNT -ge $ITERATION_LIMIT ]; then
            echo "Loop iterated ${ITERATION_LIMIT} in less than 30 seconds. Sleeping for 5 minutes..."
            sleep 300
            ITERATION_COUNT=0
        fi
    else
        ITERATION_COUNT=0
    fi
    export PREV_TIMESTAMP=$CURRENT_TIMESTAMP
}

# Run telegraf continuously with ocassional flushes:
LOOP=1
while [ $LOOP -eq 1 ]; do
    run_telegraf_continuous &
    TELEGRAF_PID=$!
    wait $TELEGRAF_PID
    # shellcheck disable=SC2181
    if [ $? -ne 0 ]; then
        echo "Telegraf exited with non-zero status. Restarting..."
        check_iteration_rate
        sleep 1
    fi
done
