#!/bin/bash

connect_string=${connect_string:-"ssh batch-explorer-user@20.236.185.167 -p 50000"}
dest_dir="/mnt/telegraf/"

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
rm -f ./telegraf
# shellcheck disable=SC2086
rclone copy --progress --sftp-host=$ip --sftp-user=$username --sftp-port=$port --sftp-key-file=~/.ssh/id_rsa ./ :sftp:${dest_dir}  --progress --multi-thread-streams=30 --transfers=30 --checkers=45
# Copy tes_performance/vm_monitor_scripts/tes_vm_monitor.once.conf tes_performance/vm_monitor_scripts/tes_vm_monitor.continuous.conf to remote
# shellcheck disable=SC2086
scp -P $port ../vm_monitor_scripts/tes_vm_monitor.once.conf $username@$ip:${dest_dir}tes_vm_monitor.once.conf
# shellcheck disable=SC2086
scp -P $port ../vm_monitor_scripts/tes_vm_monitor.continuous.conf $username@$ip:${dest_dir}tes_vm_monitor.continuous.conf

echo -e "\n\nCopying complete"

# Execute this script on the remote server:
# shellcheck disable=SC2087
# shellcheck disable=SC2086
ssh -p $port $username@$ip << EOF
    # if azure cli is not installed, install it
    if [ ! -x "\$(command -v az)" ]; then
        curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
    fi
    set -e
    rm -f ${dest_dir}telegraf
    cd ${dest_dir} && sudo docker build -t telegraf .
    sudo docker rm temp_telegraf || true
    sudo docker run -d --name temp_telegraf telegraf
    until [ "\$(sudo docker inspect -f \{\{.State.Running\}\} temp_telegraf)"=="true" ]; do
        echo "Waiting for telegraf docker image to start"
        sleep 1
    done
    sudo docker cp temp_telegraf:/app/telegraf/telegraf ${dest_dir}telegraf
    sudo docker rm temp_telegraf  || true
EOF

# Copy the telegraf binary from the remote server to the local machine
# shellcheck disable=SC2086
scp -P $port $username@$ip:${dest_dir}telegraf ./telegraf
# shellcheck disable=SC2181
if [ $? -eq 0 ]; then
    msg="Telegraf binary copied successfully, image size is $(du -h telegraf | cut -f1)"
    if [ ! -x "$(command -v gum)" ]; then
        echo "${msg}"
    else
        gum style --foreground 212 --border-foreground 212 --border double --align center --width 50 --margin "1 2" --padding "2 4" "${msg}"
    fi
else
    msg="Telegraf binary copy failed"
    # if gum is not installed just echo the message
    if [ ! -x "$(command -v gum)" ]; then
        echo "${msg}"
    else
        gum style --foreground 196 --border-foreground 196 --border double --align center --width 50 --margin "1 2" --padding "2 4" "${msg}"
    fi
    exit 1
fi
