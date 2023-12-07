#!/bin/bash

REPO_URL="https://github.com/microsoft/ga4gh-tes.git"
SOLUTION_FILE="Microsoft.GA4GH.TES.sln"
PROJECT_FILE="src/Tes.RunnerCLI/Tes.RunnerCLI.csproj"

print_green() {
    echo -e "\033[0;32m${1}\033[0m"
}

print_blue() {
    echo -e "\033[0;34m${1}\033[0m"
}

generate_random_name() {
    cat /dev/urandom | tr -dc 'a-z' | fold -w ${1:-10} | head -n 1
}

cleanup_done=0

cleanup() {
    # Change to a stable directory (e.g., home directory)
    cd ~ || exit

    # Check if cleanup has already been executed
    if [[ $cleanup_done -eq 0 ]]; then
        # Set the flag
        cleanup_done=1

        print_green "Cleaning up temporary files..."
        [ -d "$TMP_DIR" ] && rm -rf "$TMP_DIR"
        print_green "Script completed. Resource group remains active."

        # Prompt for deleting the resource group
        print_green "Do you want to delete the resource group? [Y/n]: "
        read -r -e -i "Y" response
        case "$response" in
            [yY][eE][sS]|[yY]|"")
                print_green "Deleting resource group: $RESOURCE_GROUP_NAME..."
                az group delete --name "$RESOURCE_GROUP_NAME" --yes --no-wait
                ;;
            *)
                print_green "Resource group not deleted."
                ;;
        esac
    fi
    exit
}

# Setting traps for SIGINT and EXIT
trap cleanup SIGINT
trap cleanup EXIT


TMP_DIR="/tmp/$(basename -s .git $REPO_URL)-$(date +%s)"
print_green "Using temporary directory: $TMP_DIR"

if [ "$#" -ne 3 ]; then
    print_green "Usage: $0 SUBSCRIPTION_ID REGION OWNER_TAG_VALUE"
    exit 1
fi

SUBSCRIPTION_ID=$1
REGION=$2
OWNER_TAG_VALUE=$3

az login

RESOURCE_GROUP_NAME="${OWNER_TAG_VALUE}-$(generate_random_name)"
VM_NAME=$(generate_random_name)
IDENTITY_NAME="${VM_NAME}-identity"
STORAGE_ACCOUNT_NAME="$(generate_random_name)storage"

print_green "Cloning repository $REPO_URL into $TMP_DIR..."
git clone $REPO_URL $TMP_DIR || { echo "Failed to clone the repository."; exit 1; }

cd $TMP_DIR

rm nuget.config
print_green "Building the solution..."
~/.dotnet/dotnet build $SOLUTION_FILE || { echo "Failed to build the solution."; exit 1; }

print_green "Publishing the project as a self-contained application..."
~/.dotnet/dotnet publish $PROJECT_FILE -c Release -r linux-x64 --self-contained true --output ./bin || { echo "Failed to publish the project."; exit 1; }

TES_RUNNER_BINARY="$TMP_DIR/bin/tes-runner"

echo "$json_text" > runner-task.json

print_green "Setting subscription ID..."
az account set --subscription $SUBSCRIPTION_ID

# Resource group creation
print_green "Creating resource group: $RESOURCE_GROUP_NAME in region: $REGION with owner tag: $OWNER_TAG_VALUE..."
az group create --name $RESOURCE_GROUP_NAME --location $REGION --tags OWNER=$OWNER_TAG_VALUE

# Identity creation
print_green "Creating user-assigned managed identity: $IDENTITY_NAME..."
az identity create --name $IDENTITY_NAME --resource-group $RESOURCE_GROUP_NAME

# Storage account creation
print_green "Creating storage account: $STORAGE_ACCOUNT_NAME..."
az storage account create --name $STORAGE_ACCOUNT_NAME --resource-group $RESOURCE_GROUP_NAME --location $REGION --sku Standard_LRS

# Retrieve the principal ID of the managed identity
IDENTITY_PRINCIPAL_ID=$(az identity show --name $IDENTITY_NAME --resource-group $RESOURCE_GROUP_NAME --query principalId --output tsv)

# Retrieve the resource ID of the storage account
STORAGE_ACCOUNT_RESOURCE_ID=$(az storage account show --name $STORAGE_ACCOUNT_NAME --resource-group $RESOURCE_GROUP_NAME --query id --output tsv)

# Assign the 'Storage Blob Data Contributor' role to the managed identity for the storage account
print_green "Assigning role to the managed identity..."
az role assignment create --assignee $IDENTITY_PRINCIPAL_ID --role "Storage Blob Data Owner" --scope $STORAGE_ACCOUNT_RESOURCE_ID


# Container creation
print_green "Creating container 'tes-internal' in the storage account..."
STORAGE_ACCOUNT_KEY=$(az storage account keys list --resource-group $RESOURCE_GROUP_NAME --account-name $STORAGE_ACCOUNT_NAME --query '[0].value' -o tsv)
az storage container create --name tes-internal --account-name $STORAGE_ACCOUNT_NAME --account-key $STORAGE_ACCOUNT_KEY

# VM creation
print_green "Creating virtual machine: $VM_NAME with Ubuntu 22.04..."
VM_PUBLIC_IP=$(az vm create \
    --resource-group $RESOURCE_GROUP_NAME \
    --name $VM_NAME \
    --image Ubuntu2204 \
    --admin-username azureuser \
    --generate-ssh-keys \
    --query publicIpAddress -o tsv)

IDENTITY_ID=$(az identity show --name $IDENTITY_NAME --resource-group $RESOURCE_GROUP_NAME --query id -o tsv)

print_green "Opening port 22 for SSH access..."
az vm open-port --port 22 --resource-group $RESOURCE_GROUP_NAME --name $VM_NAME

# Task definition
TASK_ID=$(uuidgen)
WORKFLOW_ID=$(uuidgen)

json_text="{
    \"Id\": \"$TASK_ID\",
    \"WorkflowId\": \"$WORKFLOW_ID\",
    \"ImageTag\": \"22.04\",
    \"ImageName\": \"ubuntu\",
    \"CommandsToExecute\": [\"echo\", \"hello world\"],
    \"MetricsFilename\": \"metrics.txt\",
    \"InputsMetricsFormat\": \"FileDownloadSizeInBytes={Size}\",
    \"OutputsMetricsFormat\": \"FileUploadSizeInBytes={Size}\",
    \"RuntimeOptions\": {
        \"NodeManagedIdentityResourceId\": \"$IDENTITY_ID\",
        \"StorageEventSink\": {
            \"TargetUrl\": \"https://$STORAGE_ACCOUNT_NAME.blob.core.windows.net/tes-internal\",
            \"TransformationStrategy\": 5
        },
        \"StreamingLogPublisher\": {
            \"TargetUrl\": \"https://$STORAGE_ACCOUNT_NAME.blob.core.windows.net/tes-internal/$TASK_ID\",
            \"TransformationStrategy\": 5
        }
    }
}"


echo "$json_text" > /tmp/runner-task.json

print_green "Uploading $TES_RUNNER_BINARY and /tmp/runner-task.json and executing them..."
scp -o StrictHostKeyChecking=no $TES_RUNNER_BINARY azureuser@$VM_PUBLIC_IP:/tmp/tes-runner
scp -o StrictHostKeyChecking=no /tmp/runner-task.json azureuser@$VM_PUBLIC_IP:/tmp/runner-task.json

ssh -o StrictHostKeyChecking=no azureuser@$VM_PUBLIC_IP "curl -H 'Metadata: true' 'http://169.254.169.254/metadata/identity/info?api-version=2021-02-01'" > /tmp/vm_identity_info.json 2>/dev/null

ssh -o StrictHostKeyChecking=no azureuser@$VM_PUBLIC_IP "chmod +x /tmp/tes-runner && /tmp/tes-runner -f /tmp/runner-task.json" > /tmp/output_and_error.txt 2>&1
echo "Done. Final output from SSH stdout and stderr:"
print_blue "$(cat /tmp/output_and_error.txt)"
print_green "Assigning the identity $IDENTITY_NAME to $VM_NAME..."
az vm identity assign -g $RESOURCE_GROUP_NAME -n $VM_NAME --identities $IDENTITY_ID
print_green "Now sleeping then will try again..."
sleep 300
ssh -o StrictHostKeyChecking=no azureuser@$VM_PUBLIC_IP "curl -H 'Metadata: true' 'http://169.254.169.254/metadata/identity/info?api-version=2021-02-01'" > /tmp/vm_identity_info_delay.json 2>/dev/null

ssh -o StrictHostKeyChecking=no azureuser@$VM_PUBLIC_IP "chmod +x /tmp/tes-runner && /tmp/tes-runner -f /tmp/runner-task.json" > /tmp/output_and_error_delay.txt 2>&1
echo "Done after sleep. Final output from SSH stdout and stderr:"
print_blue "$(cat /tmp/output_and_error_delay.txt)"
print_green "Resource Group: $RESOURCE_GROUP_NAME"
print_green "VM Name: $VM_NAME"
print_green "Identity ID: $IDENTITY_ID"


print_green "Example to run wget command on the VM: ssh azureuser@$VM_PUBLIC_IP \"/tmp/tes-runner -f /tmp/runner-task.json\""
#print_green "To delete the resource group, run:"
#print_green "az group delete --name $RESOURCE_GROUP_NAME --yes --no-wait"



echo "*** DONE ***"
print_green "VM IMDS data:/tmp/vm_imds_data.json"
print_green "stdout/err from tes-runner is here: /tmp/output_and_error.txt"
print_green "VM IMDS data:/tmp/vm_imds_data_delay.json"
print_green "stdout/err from tes-runner is here: /tmp/output_and_error_delay.txt"
print_green "tes-runner logs are here: https://$STORAGE_ACCOUNT_NAME.blob.core.windows.net/tes-internal/$TASK_ID"
echo "*** DONE ***"
