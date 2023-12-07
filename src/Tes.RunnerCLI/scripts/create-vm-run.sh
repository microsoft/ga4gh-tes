#!/bin/bash

# To run from WSL (where 'mattmcl' is your username):
# chmod +x /mnt/c/Users/mattmcl/Source/Repos/microsoft/ga4gh-tes/src/Tes.RunnerCLI/scripts/create-vm-run.sh
# /mnt/c/Users/mattmcl/Source/Repos/microsoft/ga4gh-tes/src/Tes.RunnerCLI/scripts/create-vm-run.sh

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 SUBSCRIPTION_ID REGION OWNER_TAG_VALUE"
    exit 1
fi

SUBSCRIPTION_ID=$1
REGION=$2
OWNER_TAG_VALUE=$3

generate_random_name() {
    cat /dev/urandom | tr -dc 'a-z' | fold -w ${1:-10} | head -n 1
}

RANDOM_NAME=$(generate_random_name)
RESOURCE_GROUP_NAME="${OWNER_TAG_VALUE}-${RANDOM_NAME}"
VM_NAME=$(generate_random_name)

cleanup() {
    echo
    read -p "Do you want to delete the resource group? [Y/n]: " -r -e -i "Y" response
    case "$response" in
        [yY][eE][sS]|[yY]|"")
            echo "Deleting resource group: $RESOURCE_GROUP_NAME..."
            az group delete --name $RESOURCE_GROUP_NAME --yes --no-wait
            ;;
        *)
            echo "Resource group not deleted."
            ;;
    esac
    exit
}

# Trap SIGINT (Ctrl+C) and call the cleanup function
trap cleanup SIGINT
az login
echo "Setting subscription ID..."

az account set --subscription $SUBSCRIPTION_ID

echo "Creating resource group: $RESOURCE_GROUP_NAME in region: $REGION with owner tag: $OWNER_TAG_VALUE..."

az group create --name $RESOURCE_GROUP_NAME --location $REGION --tags OWNER=$OWNER_TAG_VALUE

echo "Creating virtual machine: $VM_NAME with Ubuntu 22.04..."
# Create a virtual machine with Ubuntu 22.04
az vm create \
    --resource-group $RESOURCE_GROUP_NAME \
    --name $VM_NAME \
    --image Ubuntu2204 \
    --admin-username azureuser \
    --generate-ssh-keys
    --public-ip-sku Standard

echo "Opening port 22 for SSH access..."
# Open port 22 for SSH access
az vm open-port --port 22 --resource-group $RESOURCE_GROUP_NAME --name $VM_NAME

#echo "Running 'run.sh' on the VM..."
# Run 'echo hello world' on the VM --scripts "echo hello world"
#az vm run-command invoke \
#    --command-id RunShellScript \
#    --name $VM_NAME \
#    --resource-group $RESOURCE_GROUP_NAME \
#    --script-file run.sh 

echo "Uploading and running 'run.sh' script on the VM..."
# Upload and run 'run.sh' script from the current directory
az vm extension set \
    --resource-group exttest \
    --vm-name exttest \
    --name customScript \
    --publisher Microsoft.Azure.Extensions \
    --protected-settings '{"fileUris": ["https://raw.githubusercontent.com/microsoft/ga4gh-tes/5df9d9b478cb27ff2ac0c84e7e7a7c83a0b85ebd/src/Tes.RunnerCLI/scripts/clone-build-run.sh"],"commandToExecute": "./clone-build-run.sh"}'

# Continuously check if the script execution has completed
while true; do
    echo "Checking if script done..."
    script_status=$(az vm extension show --resource-group $RESOURCE_GROUP_NAME --vm-name $VM_NAME --name customScript --query 'instanceView.status' -o tsv)
    
    if [ "$script_status" == "Succeeded" ]; then
        echo "Script succeeded."
        break
    elif [ "$script_status" == "Failed" ]; then
        echo "Script execution on the VM has failed."
        exit 1
    fi

    sleep 2
done

echo "Downloading script output..."
az vm extension file copy \
    --resource-group $RESOURCE_GROUP_NAME \
    --vm-name $VM_NAME \
    --publisher Microsoft.Azure.Extensions \
    --extension-name customScript \
    --name /tmp/run_output.log \
    --destination ./run_output.log

# Display the content of the output file
cat run_output.log

echo "Script completed. Resource group remains active."

echo "Resource Group: $RESOURCE_GROUP_NAME"
echo "VM Name: $VM_NAME"

# Output an example az vm run-command to download Google homepage
echo "Example to run wget command on the VM:"
echo "az vm run-command invoke --command-id RunShellScript --name $VM_NAME --resource-group $RESOURCE_GROUP_NAME --scripts \"wget -O index.html http://google.com\""

# Output an example command to delete the resource group
echo "To delete the resource group, run:"
echo "az group delete --name $RESOURCE_GROUP_NAME --yes --no-wait"