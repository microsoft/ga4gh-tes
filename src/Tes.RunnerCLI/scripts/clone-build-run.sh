#!/bin/bash

# Define the repository URL
REPO_URL="https://github.com/microsoft/ga4gh-tes.git"

# Extract the repository name from the URL
REPO_DIR=$(basename -s .git $REPO_URL)

# Define the solution file path
SOLUTION_FILE="Microsoft.GA4GH.TES.sln"

# Define the project file path
PROJECT_FILE="src/Tes.RunnerCLI/Tes.RunnerCLI.csproj"

# Function to discover the path of the dotnet executable
discover_dotnet_path() {
    local dotnet_path=""
    

    # Linux: Check default installation directories
    if [ -f "/usr/share/dotnet/dotnet" ]; then
        dotnet_path="/usr/share/dotnet/dotnet"
    elif [ -f "/usr/local/share/dotnet/dotnet" ]; then
        dotnet_path="/usr/local/share/dotnet/dotnet"
    else
        echo "dotnet not found in default directories. Please specify the path manually."
        exit 1
    fi


    echo "$dotnet_path"
}

# Function to check if .NET 7 is installed
check_dotnet_version() {
    dotnet_version=$(dotnet --version 2>&1)
    if [[ $? -ne 0 ]] || [[ ! $dotnet_version == 7* ]]; then
        echo ".NET 7 is not installed. Installing..."
        # Add .NET 7 installation command based on OS
        if [[ "$OSTYPE" == "linux-gnu"* ]]; then
            # Linux installation command
            wget https://dot.net/v1/dotnet-install.sh
            chmod +x dotnet-install.sh
            ./dotnet-install.sh --channel 7.0
        elif [[ "$OSTYPE" == "darwin"* ]]; then
            # macOS installation command
            /bin/bash -c "$(curl -fsSL https://dot.net/v1/dotnet-install.sh)" --channel 7.0
        elif [[ "$OSTYPE" == "msys" ]]; then
            # Windows installation command (using PowerShell)
            powershell.exe -Command "& {Invoke-WebRequest -Uri 'https://dot.net/v1/dotnet-install.ps1' -OutFile 'dotnet-install.ps1'; .\dotnet-install.ps1 -Channel 7.0}"
        else
            echo "Unsupported operating system."
            exit 1
        fi
    else
        echo ".NET 7 is already installed."
    fi
}

# Check and install .NET 7 if needed
check_dotnet_version


# Delete the existing repository directory if it exists
if [ -d "$REPO_DIR" ]; then
    echo "Deleting existing directory $REPO_DIR..."
    rm -rf "$REPO_DIR"
fi

# Clone the repository
echo "Cloning repository $REPO_URL..."
git clone $REPO_URL
if [ $? -ne 0 ]; then
    echo "Failed to clone the repository."
    exit 1
fi

# Change directory to the cloned repository
cd $REPO_DIR

echo "Deleting nuget.config..."
rm nuget.config

# Build the solution
echo "Building the solution..."
~/.dotnet/dotnet build $SOLUTION_FILE
if [ $? -ne 0 ]; then
    echo "Failed to build the solution."
    exit 1
fi

# Build the specified project to access the binary
echo "Building the project to access the binary..."
~/.dotnet/dotnet build $PROJECT_FILE --output ./bin
if [ $? -ne 0 ]; then
    echo "Failed to build the project."
    exit 1
fi

# The binary will be located in the 'bin' directory of the cloned repository

TES_RUNNER_BINARY="$(pwd)/bin/tes-runner"
echo "Binary: $TES_RUNNER_BINARY"
chmod +x $TES_RUNNER_BINARY
$TES_RUNNER_BINARY
echo "Done"

# Script end
