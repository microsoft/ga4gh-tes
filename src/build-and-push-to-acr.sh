#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <acr_name>"
    exit 1
fi

ACR_NAME=$1
IMAGE_NAME=tes
DOCKERFILE=Dockerfile-Tes
TAG=latest

ACR_LOGIN_SERVER="${ACR_NAME}.azurecr.io"
az login
az acr login --name $ACR_NAME
docker build -t $IMAGE_NAME -f $DOCKERFILE .
docker tag $IMAGE_NAME "${ACR_LOGIN_SERVER}/${IMAGE_NAME}:${TAG}"
echo "Pushing image... ${ACR_LOGIN_SERVER}/${IMAGE_NAME}:${TAG}"
docker push "${ACR_LOGIN_SERVER}/${IMAGE_NAME}:${TAG}"

