#!/bin/bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# This script builds a new TES image, pushes it the ACR, gives AKS AcrPull on the ACR, and updates the TES deployment

RESOURCE_GROUP_NAME=$1
ACR_NAME=$2

if [ -z "$RESOURCE_GROUP_NAME" ] || [ -z "$ACR_NAME" ]; then
    echo "Usage: $0 <ResourceGroupName> <AcrName>"
    exit 1
fi

IMAGE_NAME=tes
DOCKERFILE=Dockerfile-Tes
TAG=latest
ACR_LOGIN_SERVER="${ACR_NAME}.azurecr.io"
NEW_IMAGE="${ACR_LOGIN_SERVER}/${IMAGE_NAME}:${TAG}"
az login
az acr login --name $ACR_NAME
docker build -t $NEW_IMAGE -f $DOCKERFILE .
echo "Pushing image... ${NEW_IMAGE}"
docker push $NEW_IMAGE

# Get the first AKS cluster name in the specified resource group
AKS_CLUSTER_NAME=$(az aks list --resource-group $RESOURCE_GROUP_NAME --query '[0].name' -o tsv)
if [ -z "$AKS_CLUSTER_NAME" ]; then
    echo "No AKS cluster found in resource group $RESOURCE_GROUP_NAME."
    exit 1
fi

echo "Found AKS Cluster: $AKS_CLUSTER_NAME"

# Get the managed identity client ID used by the AKS cluster
AKS_IDENTITY_CLIENT_ID=$(az aks show \
    --resource-group $RESOURCE_GROUP_NAME \
    --name $AKS_CLUSTER_NAME \
    --query identityProfile.kubeletidentity.clientId \
    -o tsv)

# Get the ACR resource ID
ACR_RESOURCE_ID=$(az acr show \
    --name $ACR_NAME \
    --query id \
    -o tsv)

# Check if the AcrPull role assignment already exists
EXISTING_ASSIGNMENT=$(az role assignment list \
    --assignee $AKS_IDENTITY_CLIENT_ID \
    --role acrpull \
    --scope $ACR_RESOURCE_ID \
    --query [].id \
    -o tsv)

if [ -z "$EXISTING_ASSIGNMENT" ]; then
    # Assign AcrPull role to the AKS cluster's managed identity for the ACR
    echo "Assigning AcrPull role to AKS..."
    az role assignment create \
        --assignee $AKS_IDENTITY_CLIENT_ID \
        --role acrpull \
        --scope $ACR_RESOURCE_ID
    echo "AcrPull role assigned to the AKS cluster successfully."
else
    echo "AcrPull role assignment already exists. No action required."
fi

# Update the AKS cluster
echo "Updating AKS with the new image..."
az aks get-credentials --resource-group $RESOURCE_GROUP_NAME --name $AKS_CLUSTER_NAME --overwrite-existing
kubectl set image deployment/tes tes=$NEW_IMAGE -n tes
echo "Deployment complete."

# Run a test task and get it's status
# Get these from TesCredentials.json after running deploy-tes-on-azure
# TesHostname="REMOVED"
# TesPassword="REMOVED"
# response=$(curl -u "tes:$TesPassword" -H "Content-Type: application/json" -X POST -d '{"resources": {"cpu_cores": 1, "ram_gb": 1},"executors":[{"image":"ubuntu","command":["/bin/sh","-c","cat /proc/sys/kernel/random/uuid"]}]}' "https://$TesHostname/v1/tasks")
# taskId=$(echo $response | jq -r '.id')
# curl -u "tes:$TesPassword" -H "Content-Type: application/json" -X GET "https://$TesHostname/v1/tasks/$taskId?view=full"
