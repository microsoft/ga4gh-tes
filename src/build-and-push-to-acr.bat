REM Copyright (c) Microsoft Corporation.
REM Licensed under the MIT License.

@echo off
SETLOCAL

IF NOT "%~1"=="" GOTO main
echo Usage: %~nx0 ^<acr_name^>
exit /b 1

:main
SET "ACR_NAME=%~1"
SET "IMAGE_NAME=tes"
SET "DOCKERFILE=Dockerfile-Tes"
SET "TAG=latest"
SET "ACR_LOGIN_SERVER=%ACR_NAME%.azurecr.io"

REM Login to Azure
call az login
IF %ERRORLEVEL% NEQ 0 (
    echo Failed to log in to Azure
    exit /b 1
)

REM Login to Azure Container Registry
call az acr login --name %ACR_NAME%
IF %ERRORLEVEL% NEQ 0 (
    echo Failed to log in to Azure Container Registry
    exit /b 1
)

REM Build and tag the Docker image
docker build -t %IMAGE_NAME% -f %DOCKERFILE% .
IF %ERRORLEVEL% NEQ 0 (
    echo Failed to build Docker image
    exit /b 1
)

docker tag %IMAGE_NAME% "%ACR_LOGIN_SERVER%/%IMAGE_NAME%:%TAG%"
IF %ERRORLEVEL% NEQ 0 (
    echo Failed to tag Docker image
    exit /b 1
)

echo Pushing image... %ACR_LOGIN_SERVER%/%IMAGE_NAME%:%TAG%

docker push "%ACR_LOGIN_SERVER%/%IMAGE_NAME%:%TAG%"
IF %ERRORLEVEL% NEQ 0 (
    echo Failed to push Docker image
    exit /b 1
)

echo Image pushed successfully.
ENDLOCAL
