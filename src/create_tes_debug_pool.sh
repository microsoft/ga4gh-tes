#!/bin/bash
## This script is for debugging of batch_scripts in TES. It will create a DEBUG pool and job in an Azure Batch
## account and then allow you to rapidly execute a task on a node that is already allocated and waiting.
## Turn around time from running this script to the job running on the node is about 30 seconds.
##
## You must have the Azure CLI installed and logged in to use this script.
## 
## Already setup pool JSON usage, pass in 3 args: SUBSCRIPTION_ID RESOURCE_GROUP WGET_URL 
##      Make sure you change the managed identity in the tes_pool_config.json 
##      ['identity']['userAssignedIdentities'][0]['resourceId'] to your own
##
## Create a pool config, pass in 5 args: SUBSCRIPTION_ID RESOURCE_GROUP WGET_URL TEMPLATE_POOL_ID POOL_CONFIG_JSON
##      Example: a0e0e744-06b2-4fd3-9230-ebf8ef1ac4c8 test-coa4-southcentral-rg 
##               https://cromwellsc95a88970e25.blob.core.windows.net/cromwell-executions/test/f7fd31e3-61e7-48b3-b895-8b291bbecbdb/call-hello/tes_task/batch_script 
##               TES-OY5BKMMX-A1_v2-2m4tvpnjrgv74kjiyxtffht2mqzd2nqn-yhlj3wwu (pool to copy from)
##               tes_pool_config.json (dest pool config file)
##
## You can also export and define these in your shell environment, and the script will use them as defaults.
## The values here won't work and are just for illutstrative purposes.
## The WGET_URL will have a SAS token added on so you do not need to add one
##
## NOTE: You must be familiar with how to remove Azure Batch pools before using this script. The script never sizes down
## the pool, so you must do this manually. Otherwise you will be charged for the VMs in the pool (which will run forever).
##
## Dependencies: Azure CLI, Python3, jq, wget + pyhton depdencies azure-batch, azure-mgmt-batch, azure-identity

# If these variables are already defined in the environment, use them, otherwise use the defaults in this script
export SUBSCRIPTION_ID=${SUBSCRIPTION_ID:-a0e0e744-06b2-4fd3-9230-ebf8ef1ac4c8}
export RESOURCE_GROUP=${RESOURCE_GROUP:-test-coa4-southcentral-rg}
export WGET_URL=${WGET_URL:-https://cromwellsc95a88970e25.blob.core.windows.net/cromwell-executions/test/f7fd31e3-61e7-48b3-b895-8b291bbecbdb/call-hello/tes_task/batch_script}

# If we were passed in 3 arguments use them:
export POOL_CONFIG_JSON="tes_pool_config.json"
if [ $# -eq 3 ]; then
    export SUBSCRIPTION_ID=$1
    export RESOURCE_GROUP=$2
    export WGET_URL=$3
elif [ $# -eq 5 ]; then
    export SUBSCRIPTION_ID=$1
    export RESOURCE_GROUP=$2
    export WGET_URL=$3
    export TEMPLATE_POOL_ID=$4
    export POOL_CONFIG_JSON=$5
fi

export JOB_ID="DEBUG_TES_JOB"
export POOL_ID="DEBUG_TES_POOL"
export DEBUG_TASK_NAME="debug_task"
export VM_SIZE="Standard_D2s_v3"
export LOW_PRI_TARGET_NODES=1

echo "REMINDER: You must manually delete the pool after you are done debugging. The script never sizes down the pool."
echo "REMINDER: This means you will be charged for 1 low-pri node until you delete the pool!"
echo -e "\n\n"

echo "Adding a job to run the batch_script in: ${WGET_URL}"
echo -e "Subscription ID: \t$SUBSCRIPTION_ID"
echo -e "Resource Group: \t$RESOURCE_GROUP"
echo -e "Job ID: \t\t$JOB_ID"
echo -e "Pool ID: \t\t$POOL_ID"
echo -e "Task Name: \t\t$DEBUG_TASK_NAME"
echo -e "VM Size: \t\t$VM_SIZE"
echo -e "Pool Config JSON: \t$POOL_CONFIG_JSON"

# Get the Azure Batch account in the resource group, error if there are more than 1
function get_az_batch_account {
    local RESOURCE_GROUP=$1
    local BATCH_ACCOUNTS=$(az batch account list --resource-group "$RESOURCE_GROUP" --query "[].name" --output tsv)
    local NUM_BATCH_ACCOUNTS=$(echo "$BATCH_ACCOUNTS" | wc -l)
    if [ "$NUM_BATCH_ACCOUNTS" -ne 1 ]; then
        echo "Error: There must be exactly 1 Azure Batch account in the resource group. Found ${NUM_BATCH_ACCOUNTS}."
        exit 1
    fi
    echo "$BATCH_ACCOUNTS"
}
export BATCH_ACCOUNT_NAME=$(get_az_batch_account "$RESOURCE_GROUP")
echo -e "Azure Batch account: \t$BATCH_ACCOUNT_NAME"
export BATCH_ACCOUNT_URL=$(az batch account show --name "$BATCH_ACCOUNT_NAME" --resource-group "$RESOURCE_GROUP" --query "accountEndpoint" --output tsv)
echo -e "Azure Batch URL: \t$BATCH_ACCOUNT_URL"
echo -e "\n\n"


# Generate a user delegation SAS token for the batch_script
function add_sas_token_to_wget_url() {
    local WGET_URL=$1
    STORAGE_ACCOUNT_NAME=$(echo "$WGET_URL" | cut -d'.' -f1 | cut -d'/' -f3)
    CONTAINER_NAME=$(echo "$WGET_URL" | cut -d'/' -f4)
    BLOB_NAME=$(echo "$WGET_URL" | cut -d'/' -f5-)
    EXPIRY=$(date -u -d "+1 day" '+%Y-%m-%dT%H:%M:%SZ')
    SAS_TOKEN=$(az storage blob generate-sas --account-name "$STORAGE_ACCOUNT_NAME" --container-name "$CONTAINER_NAME" --name "$BLOB_NAME" --permissions r --expiry "$EXPIRY" --https-only --output tsv --auth-mode login --as-user)
    echo "${WGET_URL}?${SAS_TOKEN}"
}

# If there's no SAS token in the WGET_URL, add one
if [[ $WGET_URL != *"?"* ]]; then
    echo "Generating SAS token for batch_script URL"
    export WGET_URL=$(add_sas_token_to_wget_url "$WGET_URL")
fi

# Authenticate with Azure Batch account
az batch account login --name "$BATCH_ACCOUNT_NAME" --resource-group "$RESOURCE_GROUP"
if [ -n "$TEMPLATE_POOL_ID" ]; then
    echo "Downloading pool config to template file: $POOL_CONFIG_JSON"
    az batch pool show --account-name "$BATCH_ACCOUNT_NAME" --pool-id "$TEMPLATE_POOL_ID"  > "$POOL_CONFIG_JSON"
fi

# If the pool doesn't exist, create it
# NOTE: Do not assign a principalID or ClientID to the managed identity otherwise it will not be added to the pool
# If there's already a pool
POOL_RESULT="$(az batch pool show --pool-id "$POOL_ID" --query "id" --output tsv 2>/dev/null)"
if [ "$POOL_RESULT" == "$POOL_ID" ]; then
    echo "The pool $POOL_ID already exists."
else
    echo "The pool $POOL_ID does not exist or is not in a steady state. Creating the pool..."
    # The Azure cli doesn't offer enough support for the type of pool we want so we must use the python SDK
    python3 <<EOF
from azure.identity import DefaultAzureCredential
from azure.mgmt.batch import BatchManagementClient
from azure.mgmt.batch.models import Pool, UserAssignedIdentities, ScaleSettings, FixedScaleSettings, AutoUserSpecification, UserIdentity, StartTask, BatchPoolIdentity, VirtualMachineConfiguration, ImageReference, DeploymentConfiguration, VirtualMachineConfiguration, DiskEncryptionConfiguration 
import os, json
from datetime import datetime

# Get the environment variables
SUBSCRIPTION_ID = os.environ["SUBSCRIPTION_ID"]
RESOURCE_GROUP = os.environ["RESOURCE_GROUP"]
BATCH_ACCOUNT_NAME = os.environ["BATCH_ACCOUNT_NAME"]
BATCH_ACCOUNT_URL = os.environ["BATCH_ACCOUNT_URL"]
VM_SIZE = os.environ["VM_SIZE"]
LOW_PRI_TARGET_NODES = os.environ["LOW_PRI_TARGET_NODES"]
WGET_URL = os.environ["WGET_URL"]
POOL_ID = os.environ['POOL_ID']
POOL_CONFIG_JSON = os.environ['POOL_CONFIG_JSON']

# Create a Batch management client
credential = DefaultAzureCredential()
batch_management_client = BatchManagementClient(credential, SUBSCRIPTION_ID)

# Load the parameters from tes_pool_config.json:
with open(POOL_CONFIG_JSON, 'r') as file:
    pool_config = json.load(file)

# If resizeTimeout is a quasi-date convert it to a timedelta = 'PT15M'
if isinstance(pool_config['resizeTimeout'], str):
    time_obj = datetime.strptime(pool_config['resizeTimeout'], "%H:%M:%S")
    pool_config['resizeTimeout'] = f"PT{time_obj.hour}H{time_obj.minute}M{time_obj.second}S"
    print(f"Converted resizeTimeout to {pool_config['resizeTimeout']}")

# Create a pool with a user-assigned managed identity (this may only be done with the management client)
pool = Pool(
    # id=POOL_ID,
    display_name=POOL_ID,
    vm_size=VM_SIZE,
    task_slots_per_node=pool_config['taskSlotsPerNode'],
    target_node_communication_mode=pool_config['targetNodeCommunicationMode'],
    metadata=pool_config['metadata'],
    start_task=StartTask.from_dict(pool_config['startTask']),
    scale_settings=ScaleSettings(
        fixed_scale=FixedScaleSettings(
            resize_timeout=pool_config['resizeTimeout'],
            target_dedicated_nodes=0,
            target_low_priority_nodes=LOW_PRI_TARGET_NODES,
            node_deallocation_option='Requeue') # We specifically ignore this setting
    ),
    deployment_configuration=DeploymentConfiguration(
        virtual_machine_configuration=VirtualMachineConfiguration(
            image_reference=ImageReference.from_dict(pool_config['virtualMachineConfiguration']['imageReference']),
            node_agent_sku_id=pool_config['virtualMachineConfiguration']['nodeAgentSkuId']
        ),
    ),
    identity=BatchPoolIdentity(
        type=pool_config['identity']['type'],
        user_assigned_identities={
            pool_config['identity']['userAssignedIdentities'][0]['resourceId']: UserAssignedIdentities()
        }
    )
)
# Add disk encryption settings if they are present
if ('diskEncryptionConfiguration' in pool_config['virtualMachineConfiguration'] and pool_config['virtualMachineConfiguration']['diskEncryptionConfiguration'] and 'targets' in pool_config['virtualMachineConfiguration']['diskEncryptionConfiguration']):
    pool.deployment_configuration.virtual_machine_configuration.disk_encryption_configuration = DiskEncryptionConfiguration.from_dict(pool_config['virtualMachineConfiguration']['diskEncryptionConfiguration'])
# TODO: Add data disks if they are present


batch_management_client.pool.create(
    resource_group_name=RESOURCE_GROUP, 
    account_name=BATCH_ACCOUNT_NAME, 
    pool_name=POOL_ID, parameters=pool)
print(f"Pool {POOL_ID} created on resource group {RESOURCE_GROUP}, account {BATCH_ACCOUNT_NAME}, pool {POOL_ID}")
EOF
    echo -e "\n\n"
fi

# # Check if the lowpriority target nodes is 0, if so set it to 1
# if [ $(az batch pool show --pool-id $POOL_ID --query "targetLowPriorityNodes") == 0 ] ; then
#     echo "Resizing the pool, setting the target low priority nodes to $LOW_PRI_TARGET_NODES"
#     az batch pool resize --pool-id $POOL_ID --target-low-priority-nodes $LOW_PRI_TARGET_NODES  --target-dedicated-nodes 0
# fi

# Create a new job, if the job doesn't exist
echo "Creating a job $JOB_ID in pool $POOL_ID"
az batch job show --job-id $JOB_ID >/dev/null 2>&1 || az batch job create --id $JOB_ID --pool-id $POOL_ID

# Get the task state
TASK_STATE=$(az batch task show --job-id $JOB_ID --task-id $DEBUG_TASK_NAME --query state -o tsv 2>/dev/null)

# If the task exists and is not running, delete it
if [ "$TASK_STATE" ]; then
    if [ "$TASK_STATE" != "running" ]; then
        az batch task delete --job-id $JOB_ID --task-id $DEBUG_TASK_NAME --yes
    fi
fi

# Add a task to the job (we must use python3 so we can have the task run as admin)
echo "Adding task $DEBUG_TASK_NAME to job $JOB_ID"
export PRIMARY_KEY=$(az batch account keys list --name "$BATCH_ACCOUNT_NAME" --resource-group "$RESOURCE_GROUP" --query "primary" -o tsv)

python3 <<EOF
from azure.batch import BatchServiceClient, batch_auth
from azure.batch.models import CloudTask, UserIdentity, AutoUserSpecification, ElevationLevel
import os

# Get the environment variables
BATCH_ACCOUNT_NAME = os.environ["BATCH_ACCOUNT_NAME"]
BATCH_ACCOUNT_URL = os.environ["BATCH_ACCOUNT_URL"]
DEBUG_TASK_NAME = os.environ["DEBUG_TASK_NAME"]
WGET_URL = os.environ["WGET_URL"]
JOB_ID = os.environ['JOB_ID']

PRIMARY_KEY = os.environ["PRIMARY_KEY"]
credential = batch_auth.SharedKeyCredentials(BATCH_ACCOUNT_NAME, PRIMARY_KEY)

# Normal bash command:
CMD = '/bin/bash -c "wget --https-only --no-verbose --timeout=20 --waitretry=1 --tries=9 --retry-connrefused --continue -O \$AZ_BATCH_TASK_DIR/batch_script \''
CMD = CMD + f"{WGET_URL}" + '\' && chmod +x \$AZ_BATCH_TASK_DIR/batch_script && \$AZ_BATCH_TASK_DIR/batch_script"'

# Create a Batch service client
batch_service_client = BatchServiceClient(credential, f"https://{BATCH_ACCOUNT_URL}")

# Create a user identity with admin elevation level
user_identity = UserIdentity(auto_user=AutoUserSpecification(elevation_level=ElevationLevel.admin))

# Create a task
task = CloudTask(id=DEBUG_TASK_NAME, command_line=CMD, user_identity=user_identity)

# Add the task to the job
batch_service_client.task.add(JOB_ID, task)

print(f"Task {DEBUG_TASK_NAME} added to job {JOB_ID}")
EOF

