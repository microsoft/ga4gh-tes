#!/usr/bin/bash
# Add nvme device mounting (software RAID and mount all nvme devices to the task working directory ${AZ_BATCH_NODE_ROOT_DIR}/workitems)
# Note: this nvme mounting will only work for freshly booted machines with no existing RAID arrays
#       for testing purposes it will not work if re-run on the same machine
# TODO: consider parts of https://learn.microsoft.com/azure/virtual-machines/enable-nvme-temp-faqs#how-can-i-format-and-initialize-temp-nvme-disks-in-linux-
trap "echo Error trapped; exit 0" ERR
# set -e will cause any error to exit the script
set -e
# Get nvme device paths without jq being installed
nvme_devices=$(nvme list -o json | grep -oP "\"DevicePath\" : \"\K[^\"]+" || true)
nvme_device_count=$(echo $nvme_devices | wc -w)
if [ -z "$nvme_devices" ]; then
    echo "No NVMe devices found"
else
    # Mount all nvme devices as RAID0 (striped) onto /mnt/batch/tasks/workitems using XFS
    md_device="/dev/md0"
    mdadm --create $md_device --level=0 --force --raid-devices=$nvme_device_count $nvme_devices
    # Partition and format using XFS.
    partition="${md_device}p1"
    parted "${md_device}" --script mklabel gpt mkpart xfspart xfs 0% 100%
    mkfs.xfs "${partition}"
    partprobe "${partition}"
    # Save permissions of the existing /mnt/batch/tasks/workitems/
    mount_dir="${AZ_BATCH_NODE_ROOT_DIR}/workitems"
    permissions=$(stat -c "%a" $mount_dir)
    owner=$(stat -c "%U" $mount_dir)
    group=$(stat -c "%G" $mount_dir)
    # Mount the new nvme array at /mnt/batch/tasks/workitems/
    rm -rf "${mount_dir}"
    mkdir -p "${mount_dir}"
    mount "${partition}" "${mount_dir}"
    chown $owner:$group "${mount_dir}"
    chmod $permissions "${mount_dir}"
    echo "Mounted $nvme_device_count drives to $mount_dir: $nvme_devices"
fi
