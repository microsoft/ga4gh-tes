#!/bin/bash

## Use /proc/uptime to get the system uptime in seconds (with 2 decimal places)
## This is almost as accurate as using the C function clock_gettime(CLOCK_BOOTTIME, &ts_boot);

unix_timestamp_to_iso_time_string() {
    # Convert the numeric unix timestamp to an ISO 8601 timestamp string
    iso_timestamp=$(date -u -d @"$1" +"%Y-%m-%dT%H:%M:%S.%NZ")
    echo "$iso_timestamp"
}

get_linux_boot_iso_timestamp() {
    # Get the uptime in seconds (first value from /proc/uptime)
    uptime=$(awk '{print $1}' /proc/uptime)
    # Get the (numeric) current time in seconds since the Unix epoch
    current_time=$(date +%s.%N)
    # Subtract the uptime from the current time to get a timestamp
    boot_time=$(echo "$current_time - $uptime" | bc)

    # Convert the boot time to an ISO 8601 timestamp
    boot_iso_time=$(unix_timestamp_to_iso_time_string "$boot_time")
    collection_iso_time=$(unix_timestamp_to_iso_time_string "$current_time")

    # echo "linux_boot iso_boot_timestamp=\"$boot_iso_time\",collection_iso_timestamp=\"$collection_iso_time\" $current_time"
    echo "linux_boot iso_boot_timestamp=\"$boot_iso_time\",collection_iso_timestamp=\"$collection_iso_time\""
}

# Call the function
get_linux_boot_iso_timestamp
