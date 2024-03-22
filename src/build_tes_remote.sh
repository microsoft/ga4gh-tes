#!/bin/bash

connect_string=${connect_string:-"ssh batch-explorer-user@20.236.185.167 -p 50000"}
acr_name=${acr_name:-"wdltest"}
resource_group=${resource_group:-"test-coa4-southcentral-rg"}

dest_dir="/mnt/tes/"

# Parse the Azure Batch connect string:
username=$(echo "$connect_string" | cut -d'@' -f1 | cut -d' ' -f2)
ip=$(echo "$connect_string" | cut -d'@' -f2 | cut -d' ' -f1)
port=$(echo "$connect_string" | cut -d' ' -f4)

echo -e "Username: \t$username"
echo -e "IP: \t\t$ip"
echo -e "Port: \t\t$port"

# rclone current directory to remote server
# shellcheck disable=SC2086
ssh -p $port $username@$ip "sudo mkdir -p $dest_dir && sudo chmod a+rw $dest_dir"
# shellcheck disable=SC2086
rclone copy --progress --sftp-host=$ip --sftp-user=$username --sftp-port=$port --sftp-key-file=~/.ssh/id_rsa ../ :sftp:${dest_dir}  --progress --multi-thread-streams=30 --transfers=30 --checkers=45

# Execute this script on the remote server as root:
# shellcheck disable=SC2087
# shellcheck disable=SC2086
ssh -p $port $username@$ip << EOF sudo -s
    # if azure cli is not installed, install it
    if [ ! -x "\$(command -v az)" ]; then
        curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
    fi
    set -e
    az login --identity
    cd ${dest_dir}/src && chmod a+x ./deploy-tes-docker-image.sh && ./deploy-tes-docker-image.sh $resource_group $acr_name
EOF
