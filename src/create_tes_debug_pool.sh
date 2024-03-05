#!/bin/bash

## This script is for debugging of batch_scripts in TES. It will create a DEBUG pool and job in an Azure Batch
## account and then allow you to rapidly execute a task on a node that is already allocated and waiting.
## Turn around time for adding a task to the job is about 30 seconds.
##
## You must have the Azure CLI installed and logged in to use this script.
## 
## Usage, pass in 3 args: SUBSCRIPTION_ID RESOURCE_GROUP WGET_URL 
## Make sure you change the managed identity in the tes_pool_config.json 
## ['identity']['userAssignedIdentities'][0]['resourceId'] to your own
##
## You can also export and define these in your shell environment, and the script will use them as defaults.
## The values here won't work and are just for illutstrative purposes.
## The WGET_URL will have a SAS token added on so you do not need to add one
##
## NOTE: You must be familiar with how to remove Azure Batch pools before using this script. The script never sizes down
## the pool, so you must do this manually. Otherwise you will be charged for the VMs in the pool (which will run forever).
##

# If these variables are already defined in the environment, use them, otherwise use the defaults in this script
export SUBSCRIPTION_ID=${SUBSCRIPTION_ID:-a0e0e744-06b2-4fd3-9230-ebf8ef1ac4c8}
export RESOURCE_GROUP=${RESOURCE_GROUP:-test-coa4-southcentral-rg}
export WGET_URL=${WGET_URL:-https://cromwellsc95a88970e25.blob.core.windows.net/cromwell-executions/test/32c33212-3744-411b-b083-246a34a9832c/call-hello/tes_task/batch_script}

# If we were passed in 3 arguments use them:
if [ $# -eq 3 ]; then
    export SUBSCRIPTION_ID=$1
    export RESOURCE_GROUP=$2
    export WGET_URL=$3
fi

export JOB_ID="TES_DEBUG_JOB"
export POOL_ID="TES_DEBUG_POOL"
export DEBUG_TASK_NAME="debug_task"
VM_SIZE="Standard_D2s_v3"
LOW_PRI_TARGET_NODES=1

echo "REMINDER: You must manually delete the pool after you are done debugging. The script never sizes down the pool."
echo "REMINDER: This means you will be charged for 1 low-pri node until you delete the pool!"
echo -e "\n\n"

echo "Adding a job to run the batch_script in: ${WGET_URL}"
echo -e "Subscription ID: \t$SUBSCRIPTION_ID"
echo -e "Resource Group: \t$RESOURCE_GROUP"
echo -e "Job ID: \t\t$JOB_ID"
echo -e "Pool ID: \t\t$POOL_ID"
echo -e "Task Name: \t\t$DEBUG_TASK_NAME"
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
echo -e "Azure Batch account name: \t$BATCH_ACCOUNT_NAME"
export BATCH_ACCOUNT_URL=$(az batch account show --name "$BATCH_ACCOUNT_NAME" --resource-group "$RESOURCE_GROUP" --query "accountEndpoint" --output tsv)
echo -e "Azure Batch account URL: \t$BATCH_ACCOUNT_URL"

# Authenticate with Azure Batch account
az batch account login --name "$BATCH_ACCOUNT_NAME" --resource-group "$RESOURCE_GROUP"

# Create a copy of tes_pool_config.json and do the following replacements:
# - Replace "TES_POOL_NAME" with $POOL_ID
# - Replace "TES_VM_SIZE" with $VM_SIZE
# - Replace "TES_LOW_PRIORITY_NODES" with LOW_PRI_TARGET_NODES
sed -e "s/TES_POOL_NAME/$POOL_ID/g" -e "s/TES_VM_SIZE/$VM_SIZE/g" -e "s/TES_LOW_PRIORITY_NODES/$LOW_PRI_TARGET_NODES/g" tes_pool_config.json > tes_pool_config_TMP.json

# If the pool doesn't exist, create it
# NOTE: Do not assign a principalID or ClientID to the managed identity otherwise it will not be added to the pool
# az batch pool show --pool-id $POOL_ID >/dev/null 2>&1 || az batch pool create --json-file tes_pool_config_TMP.json
# If there's already a pool
if [ "$(az batch pool show --pool-id "$POOL_ID" --query "id" --output tsv)" == "$POOL_ID" ]; then
    echo "The pool $POOL_ID already exists."
else
    echo "The pool $POOL_ID does not exist or is not in a steady state. Creating the pool..."
    # az batch pool create --json-file tes_pool_config_TMP.json
    python3 <<EOF
from azure.identity import DefaultAzureCredential
from azure.mgmt.batch import BatchManagementClient
from azure.mgmt.batch.models import Pool, UserAssignedIdentities, ScaleSettings, FixedScaleSettings, AutoUserSpecification, UserIdentity, StartTask, BatchPoolIdentity, VirtualMachineConfiguration, ImageReference, DeploymentConfiguration, VirtualMachineConfiguration
import os, json

# Get the environment variables
SUBSCRIPTION_ID = os.environ["SUBSCRIPTION_ID"]
RESOURCE_GROUP = os.environ["RESOURCE_GROUP"]
BATCH_ACCOUNT_NAME = os.environ["BATCH_ACCOUNT_NAME"]
BATCH_ACCOUNT_URL = os.environ["BATCH_ACCOUNT_URL"]
WGET_URL = os.environ["WGET_URL"]
POOL_ID = os.environ['POOL_ID']

# Create a Batch management client
credential = DefaultAzureCredential()
batch_management_client = BatchManagementClient(credential, SUBSCRIPTION_ID)

# Load the parameters from tes_pool_config_temp.json:
with open('tes_pool_config_TMP.json', 'r') as file:
    pool_config = json.load(file)

# Create a pool with a user-assigned managed identity (this may only be done with the management client)
pool = Pool(
    # id=POOL_ID,
    display_name=POOL_ID,
    vm_size=pool_config['vmSize'],
    task_slots_per_node=pool_config['taskSlotsPerNode'],
    target_node_communication_mode=pool_config['targetNodeCommunicationMode'],
    metadata=pool_config['metadata'],
    start_task=StartTask(
        command_line=pool_config['startTask']['commandLine'],
        user_identity=UserIdentity(
            auto_user=AutoUserSpecification(scope='pool', elevation_level='admin')
        ),
        max_task_retry_count=pool_config['startTask']['maxTaskRetryCount'],
        wait_for_success=pool_config['startTask']['waitForSuccess']
    ),
    scale_settings=ScaleSettings(
        fixed_scale=FixedScaleSettings(
            resize_timeout=pool_config['resizeTimeout'],
            target_dedicated_nodes=0,
            target_low_priority_nodes=pool_config['targetLowPriorityNodes'],
            node_deallocation_option='Requeue') # We specifically ignore this setting
    ),
    deployment_configuration=DeploymentConfiguration(
        virtual_machine_configuration=VirtualMachineConfiguration(
            image_reference=ImageReference(
                publisher=pool_config['virtualMachineConfiguration']['imageReference']['publisher'],
                offer=pool_config['virtualMachineConfiguration']['imageReference']['offer'],
                sku=pool_config['virtualMachineConfiguration']['imageReference']['sku'],
                version='latest'
            ),
            node_agent_sku_id=pool_config['virtualMachineConfiguration']['nodeAgentSKUId']
        ),
    ),
    identity=BatchPoolIdentity(
        type=pool_config['identity']['type'],
        user_assigned_identities={
            pool_config['identity']['userAssignedIdentities'][0]['resourceId']: UserAssignedIdentities()
        }
    )
)

batch_management_client.pool.create(
    resource_group_name=RESOURCE_GROUP, 
    account_name=BATCH_ACCOUNT_NAME, 
    pool_name=POOL_ID, parameters=pool)
EOF
fi

# # Check if the lowpriority target nodes is 0, if so set it to 1
# if [ $(az batch pool show --pool-id $POOL_ID --query "targetLowPriorityNodes") == 0 ] ; then
#     echo "Resizing the pool, setting the target low priority nodes to $LOW_PRI_TARGET_NODES"
#     az batch pool resize --pool-id $POOL_ID --target-low-priority-nodes $LOW_PRI_TARGET_NODES  --target-dedicated-nodes 0
# fi

# Create a new job, if the job doesn't exist
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
