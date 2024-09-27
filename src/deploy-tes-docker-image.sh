#!/bin/bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# This script builds a new TES image, pushes it the ACR, gives AKS AcrPull on the ACR, and updates the TES deployment

# if we are passed three arguments, use them as the resource group and ACR name
IS_US_GOVERNMENT=${IS_US_GOVERNMENT:-false}
if [ $# -ge 2 ]; then
    RESOURCE_GROUP_NAME=$1
    ACR_NAME=$2
    if [ $# -ge 3 ]; then
        IS_US_GOVERNMENT=$3
    fi
fi
if [ -z "$RESOURCE_GROUP_NAME" ] || [ -z "$ACR_NAME" ]; then
    echo "Usage: $0 <ResourceGroupName> <AcrName> [IsUsGovernment]"
    exit 1
fi

IMAGE_NAME=tes
DOCKERFILE=Dockerfile-Tes
TAG=$(date +%Y-%m-%d-%H-%M-%S)
if [[ "$IS_US_GOVERNMENT" == "true" ]]; then
    ACR_LOGIN_SERVER="${ACR_NAME}.azurecr.us" # Adjusted for US Government cloud
else
    ACR_LOGIN_SERVER="${ACR_NAME}.azurecr.io" # Default for public cloud
fi
NEW_IMAGE="${ACR_LOGIN_SERVER}/${IMAGE_NAME}:${TAG}"

# Do the docker build step:
docker build -t "$NEW_IMAGE" -f "$DOCKERFILE" .
if [ $? -ne 0 ]; then
    echo "Docker build failed"
    exit 1
fi
echo "Built image: ${NEW_IMAGE}"

echo "Pushing image... ${NEW_IMAGE}"
# Check if we're already logged in
az account show > /dev/null 2>&1
if [ $? -ne 0 ]; then
    # We're not logged in, so run the az login command
    az login
fi
az acr login --name "$ACR_NAME"
docker push "$NEW_IMAGE"

echo -e "\n\nYou can manually run: kubectl set image deployment/tes tes=\"$NEW_IMAGE\" -n coa\n\n"

echo "Attempting to update the AKS cluster with the new image..."
# Get the subscription ID of the resource group:
SUBSCRIPTION_ID=$(az group show --name "$RESOURCE_GROUP_NAME" --query "id" -o tsv | cut -d'/' -f3)
if [ -z "$SUBSCRIPTION_ID" ]; then
    echo "Failed to get the subscription ID of the resource group $RESOURCE_GROUP_NAME."
    exit 1
fi

echo "Setting active subscription to $SUBSCRIPTION_ID"
az account set --subscription "$SUBSCRIPTION_ID"

# Get the first AKS cluster name in the specified resource group
AKS_CLUSTER_NAME=$(az aks list --resource-group "$RESOURCE_GROUP_NAME" --subscription "$SUBSCRIPTION_ID" --query '[0].name' -o tsv)
if [ -z "$AKS_CLUSTER_NAME" ]; then
    echo "No AKS cluster found in resource group $RESOURCE_GROUP_NAME."
    echo "This identity does not have access to any AKS clusters in the specified resource group. Please make sure the identity has Kubernetes access."
    echo "az credential is: $(az account show -o yaml)"
    exit 1
fi
echo "Found AKS Cluster: $AKS_CLUSTER_NAME"

# Get the managed identity client ID used by the AKS cluster
AKS_IDENTITY_CLIENT_ID=$(az aks show --resource-group "$RESOURCE_GROUP_NAME" --subscription "$SUBSCRIPTION_ID" --name "$AKS_CLUSTER_NAME" --query identityProfile.kubeletidentity.clientId -o tsv)
# If there's an error, the above command will return an empty string
if [ -z "$AKS_IDENTITY_CLIENT_ID" ]; then
    echo "Failed to get the managed identity client ID used by the AKS cluster $AKS_CLUSTER_NAME."
    exit 1
fi

# Get the ACR resource ID
ACR_RESOURCE_ID=$(az acr show --subscription "$SUBSCRIPTION_ID" --name "$ACR_NAME" --query id -o tsv)
echo "ACR_RESOURCE_ID: $ACR_RESOURCE_ID"

# Check if the AcrPull role assignment already exists
EXISTING_ASSIGNMENT=$(az role assignment list \
    --assignee "$AKS_IDENTITY_CLIENT_ID" \
    --role acrpull \
    --scope "$ACR_RESOURCE_ID" \
    --query [].id \
    -o tsv)
if [ -z "$EXISTING_ASSIGNMENT" ]; then
    echo "Failed to get the managed identity client ID used by the AKS cluster $AKS_CLUSTER_NAME."
    exit 1
fi

if [ -z "$EXISTING_ASSIGNMENT" ]; then
    # Assign AcrPull role to the AKS cluster's managed identity for the ACR
    echo "Assigning AcrPull role to AKS..."
    az role assignment create \
        --assignee "$AKS_IDENTITY_CLIENT_ID" \
        --role acrpull \
        --scope "$ACR_RESOURCE_ID"
    echo "AcrPull role assigned to the AKS cluster successfully."
else
    echo "AcrPull role assignment already exists. No action required."
fi

# Update the AKS cluster with the new TES image
echo "Updating AKS with the new image..."
az aks get-credentials --resource-group "$RESOURCE_GROUP_NAME" --subscription "$SUBSCRIPTION_ID" --name "$AKS_CLUSTER_NAME" --overwrite-existing
kubectl set image deployment/tes tes="$NEW_IMAGE" -n coa
echo "Deployment complete for: $NEW_IMAGE"

# Get logs of the new TES pod
# kubectl get pods -n tes | awk '{print $1}' | xargs -I {} kubectl logs -n tes {}

# Run a test task and get it's status (Get these from TesCredentials.json after running deploy-tes-on-azure)
# TesHostname="REMOVED"
# TesPassword="REMOVED"

# response=$(curl -u "tes:$TesPassword" -H "Content-Type: application/json" -X POST -d '{"resources": {"cpu_cores": 1, "ram_gb": 1},"executors":[{"image":"ubuntu","command":["/bin/sh","-c","cat /proc/sys/kernel/random/uuid"]}]}' "https://$TesHostname/v1/tasks")
# taskId=$(echo $response | jq -r '.id')
# curl -u "tes:$TesPassword" -H "Content-Type: application/json" -X GET "https://$TesHostname/v1/tasks/$taskId?view=full"
