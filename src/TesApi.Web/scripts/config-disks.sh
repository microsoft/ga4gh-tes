#!/usr/bin/bash
# Add data disk mounting (software RAID and mount all attached devices to the task working directory ${AZ_BATCH_NODE_ROOT_DIR}/tasks/workitems)
# Note: this is based on config-nvme.sh
#       for testing purposes it will not work if re-run on the same machine
trap "echo Error trapped; exit 0" ERR
# set -e will cause any error to exit the script
set -e
# Get disk device paths without jq being installed
disk_devices=`lsblk -o NAME,HCTL | awk '
NR > 1 {
    if ($1 ~ /^sd*/ && $2 ~ /^{HctlPrefix}:*/) {
        printf "/dev/%s ", $1
    }
}'`
disk_device_count=$(echo $disk_devices | wc -w)
if [ -z "$disk_devices" ]; then
    echo "No Disk devices found"
else
    # Mount all disk devices as RAID0 (striped) onto /mnt/batch/tasks/workitems using XFS
    md_device="/dev/md0"
    mdadm --create $md_device --level=0 --force --raid-devices=$disk_device_count $disk_devices
    # Partition and format using XFS.
    partition="${md_device}p1"
    parted "${md_device}" --script mklabel gpt mkpart xfspart xfs 0% 100%
    mkfs.xfs "${partition}"
    partprobe "${partition}"
    # Save permissions of the existing /mnt/batch/tasks/workitems/
    mount_dir="${AZ_BATCH_NODE_ROOT_DIR}/tasks/workitems"
    permissions=$(stat -c "%a" $mount_dir)
    owner=$(stat -c "%U" $mount_dir)
    group=$(stat -c "%G" $mount_dir)
    # Mount the new disk array at /mnt/batch/tasks/workitems/
    rm -rf "${mount_dir}"
    mkdir -p "${mount_dir}"
    mount "${partition}" "${mount_dir}"
    chown $owner:$group "${mount_dir}"
    chmod $permissions "${mount_dir}"
    echo "Mounted $disk_device_count drives to $mount_dir: $disk_devices"
fi
