#!/bin/bash

# Create a local user named "scriptuser"
sudo useradd -m scriptuser

# Define a command to include the function and its execution
run_as_scriptuser_cmd() {
    cat <<'EOF'
# Function to execute as scriptuser
run_as_scriptuser() {
    # Redirect stdout and stderr for the entire function
    exec > /tmp/stdout.txt 2> /tmp/stderr.txt

    # Check for correct number of arguments
    if [ "\$#" -ne 2 ]; then
        echo "Usage: \$0 IDENTITY STORAGE_ACCOUNT_NAME"
        exit 1
    fi

    IDENTITY=\$1
    STORAGE_ACCOUNT_NAME=\$2

    # Define the repository URL
    REPO_URL="https://github.com/microsoft/ga4gh-tes.git"

    # Extract the repository name from the URL
    REPO_DIR=\$(basename -s .git \$REPO_URL)

    # Define the solution file path
    SOLUTION_FILE="Microsoft.GA4GH.TES.sln"

    # Define the project file path
    PROJECT_FILE="src/Tes.RunnerCLI/Tes.RunnerCLI.csproj"

    # Function to check if .NET 7 is installed
    check_dotnet_version() {
        dotnet_version=\$(dotnet --version 2>&1)
        if [[ \$? -ne 0 ]] || [[ ! \$dotnet_version == 7* ]]; then
            echo ".NET 7 is not installed. Installing..."
            # Add .NET 7 installation command based on OS
            wget https://dot.net/v1/dotnet-install.sh
            chmod +x dotnet-install.sh
            ./dotnet-install.sh --channel 7.0 --install-dir ~
        else
            echo ".NET 7 is already installed."
        fi
    }

    # Check and install .NET 7 if needed
    check_dotnet_version

    # Delete the existing repository directory if it exists
    if [ -d "\$REPO_DIR" ]; then
        echo "Deleting existing directory \$REPO_DIR..."
        rm -rf "\$REPO_DIR"
    fi

    # Clone the repository
    echo "Cloning repository \$REPO_URL..."
    git clone \$REPO_URL
    if [ \$? -ne 0 ]; then
        echo "Failed to clone the repository."
        exit 1
    fi

    # Change directory to the cloned repository
    cd \$REPO_DIR

    echo "Deleting nuget.config..."
    rm nuget.config

    # Build the solution
    echo "Building the solution..."
    ~/.dotnet/dotnet build \$SOLUTION_FILE
    if [ \$? -ne 0 ]; then
        echo "Failed to build the solution."
        exit 1
    fi

    # Build the specified project to access the binary
    echo "Building the project to access the binary..."
    ~/.dotnet/dotnet build \$PROJECT_FILE --output ./bin
    if [ \$? -ne 0 ]; then
        echo "Failed to build the project."
        exit 1
    fi

    TASK_ID=\$(uuidgen)
    WORKFLOW_ID=\$(uuidgen)

    json_text="{
      \"Id\": \"\$TASK_ID\",
      \"WorkflowId\": \"\$WORKFLOW_ID\",
      \"ImageTag\": \"22.04\",
      \"ImageName\": \"ubuntu\",
      \"CommandsToExecute\": [ \"echo\", \"hello world\" ],
      \"MetricsFilename\": \"metrics.txt\",
      \"InputsMetricsFormat\": \"FileDownloadSizeInBytes={Size}\",
      \"OutputsMetricsFormat\": \"FileUploadSizeInBytes={Size}\",
      \"RuntimeOptions\": {
        \"NodeManagedIdentityResourceId\": \"\$IDENTITY\",
        \"StorageEventSink\": {
          \"TargetUrl\": \"https://\$STORAGE_ACCOUNT_NAME.blob.core.windows.net/tes-internal\",
          \"TransformationStrategy\": 5
        },
        \"StreamingLogPublisher\": {
          \"TargetUrl\": \"https://\$STORAGE_ACCOUNT_NAME.blob.core.windows.net/tes-internal/\$TASK_ID\",
          \"TransformationStrategy\": 5
        }
      }
    }"

    echo "Writing logs to https://\$STORAGE_ACCOUNT_NAME.blob.core.windows.net/tes-internal/\$TASK_ID"
    echo "\$json_text" > runner-task.json

    # The binary will be located in the 'bin' directory of the cloned repository
    TES_RUNNER_BINARY="\$(pwd)/bin/tes-runner"
    echo "Binary: \$TES_RUNNER_BINARY"
    chmod +x \$TES_RUNNER_BINARY
    \$TES_RUNNER_BINARY runner-task.json
    echo "Done"
}

# Call the function with arguments
run_as_scriptuser "\$@"
EOF
}

# Export the function and execute it with sudo as scriptuser
export -f run_as_scriptuser_cmd
sudo -u scriptuser -i bash -c "$(run_as_scriptuser_cmd) '$1' '$2'"
