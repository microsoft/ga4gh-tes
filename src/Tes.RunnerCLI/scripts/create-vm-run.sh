#!/bin/bash

print_green() {
    local message="$1"
    echo -e "\033[0;32m${message}\033[0m"
}

# To run from WSL (where 'user' is your username):
# chmod +x /mnt/c/Users/user/Source/Repos/microsoft/ga4gh-tes/src/Tes.RunnerCLI/scripts/create-vm-run.sh
# /mnt/c/Users/user/Source/Repos/microsoft/ga4gh-tes/src/Tes.RunnerCLI/scripts/create-vm-run.sh SUBSCRIPTION_ID REGION OWNER_TAG_VALUE

if [ "$#" -ne 3 ]; then
    print_green "Usage: $0 SUBSCRIPTION_ID REGION OWNER_TAG_VALUE"
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
IDENTITY_NAME="${VM_NAME}-identity"
STORAGE_ACCOUNT_NAME="${RANDOM_NAME}storage"

cleanup() {
    print_green
    read -p "Do you want to delete the resource group? [Y/n]: " -r -e -i "Y" response
    case "$response" in
        [yY][eE][sS]|[yY]|"")
            print_green "Deleting resource group: $RESOURCE_GROUP_NAME..."
            az group delete --name $RESOURCE_GROUP_NAME --yes --no-wait
            ;;
        *)
            print_green "Resource group not deleted."
            ;;
    esac
    exit
}

# Trap SIGINT (Ctrl+C) and call the cleanup function
trap cleanup SIGINT

az login
print_green "Setting subscription ID..."
az account set --subscription $SUBSCRIPTION_ID

print_green "Creating resource group: $RESOURCE_GROUP_NAME in region: $REGION with owner tag: $OWNER_TAG_VALUE..."
az group create --name $RESOURCE_GROUP_NAME --location $REGION --tags OWNER=$OWNER_TAG_VALUE

print_green "Creating user-assigned managed identity: $IDENTITY_NAME..."
az identity create --name $IDENTITY_NAME --resource-group $RESOURCE_GROUP_NAME

print_green "Creating storage account: $STORAGE_ACCOUNT_NAME..."
az storage account create --name $STORAGE_ACCOUNT_NAME --resource-group $RESOURCE_GROUP_NAME --location $REGION --sku Standard_LRS

print_green "Creating container 'tes-internal' in the storage account..."
STORAGE_ACCOUNT_KEY=$(az storage account keys list --resource-group $RESOURCE_GROUP_NAME --account-name $STORAGE_ACCOUNT_NAME --query '[0].value' -o tsv)
az storage container create --name tes-internal --account-name $STORAGE_ACCOUNT_NAME --account-key $STORAGE_ACCOUNT_KEY

print_green "Creating virtual machine: $VM_NAME with Ubuntu 22.04..."
VM_PUBLIC_IP=$(az vm create \
    --resource-group $RESOURCE_GROUP_NAME \
    --name $VM_NAME \
    --image Ubuntu2204 \
    --admin-username azureuser \
    --generate-ssh-keys \
    --query publicIpAddress -o tsv)

IDENTITY_ID=$(az identity show --name $IDENTITY_NAME --resource-group $RESOURCE_GROUP_NAME --query id -o tsv)
print_green "Assigning the identity $IDENTITY_NAME to $VM_NAME..."
az vm identity assign -g $RESOURCE_GROUP_NAME -n $VM_NAME --identities $IDENTITY_ID

print_green "Opening port 22 for SSH access..."
az vm open-port --port 22 --resource-group $RESOURCE_GROUP_NAME --name $VM_NAME

print_green "Running 'clone-build-run.sh' script on the VM..."
az vm extension set \
    --resource-group $RESOURCE_GROUP_NAME \
    --vm-name $VM_NAME \
    --name customScript \
    --publisher Microsoft.Azure.Extensions \
    --protected-settings "{\"fileUris\": [\"https://raw.githubusercontent.com/microsoft/ga4gh-tes/mattmcl4475/handleIdentityUnavailable/src/Tes.RunnerCLI/scripts/clone-build-run.sh\"],\"commandToExecute\": \"./clone-build-run.sh $IDENTITY $STORAGE_ACCOUNT_NAME\"}"

elapsed_seconds=0

while true; do
    print_green "Checking if script done..."

    script_status=$(az vm extension show --resource-group $RESOURCE_GROUP_NAME --vm-name $VM_NAME --name customScript --query 'provisioningState' -o tsv)
    
    if [ "$script_status" == "Succeeded" ]; then
        print_green "Extension applied successfully. Checking script output for completion status..."
        break
    elif [ "$script_status" == "Failed" ]; then
        print_green "Extension application failed."
        exit 1
    fi

    print_green "Current provisioning state of the extension: $script_status"
    print_green "Elapsed time: $elapsed_seconds seconds."
    sleep 10
    elapsed_seconds=$((elapsed_seconds + 10))
done


print_green "Downloading script outputs..."
scp -o StrictHostKeyChecking=no azureuser@$VM_PUBLIC_IP:/tmp/stdout.txt ./stdout.txt
scp -o StrictHostKeyChecking=no azureuser@$VM_PUBLIC_IP:/tmp/stderr.txt ./stderr.txt
cat stdout.txt
cat stderr.txt
print_green "Script completed. Resource group remains active."
print_green "Resource Group: $RESOURCE_GROUP_NAME"
print_green "VM Name: $VM_NAME"

print_green "Example to run wget command on the VM:"
print_green "az vm run-command invoke --command-id RunShellScript --name $VM_NAME --resource-group $RESOURCE_GROUP_NAME --scripts \"wget -O index.html http://google.com\""

print_green "To delete the resource group, run:"
print_green "az group delete --name $RESOURCE_GROUP_NAME --yes --no-wait"
