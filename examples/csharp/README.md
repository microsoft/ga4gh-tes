# TES C# SDK Demonstration
We recommend opening the TES solution via the solution file in this repo ([Microsoft.GA4GH.TES.sln](https://github.com/microsoft/ga4gh-tes/blob/main/Microsoft.GA4GH.TES.sln) in [Visual Studio](https://visualstudio.microsoft.com/).  This example uses the [TES.SDK.Examples](https://github.com/microsoft/ga4gh-tes/tree/main/src/Tes.SDK.Examples) console app project in the solution. 

## Client Requirements

Make sure you have the following requirements in place to run the TES SDK examples:

1. **.NET 8.0 SDK or higher**: Install the .NET SDK to build and run the C# examples.

   [Download .NET SDK](https://dotnet.microsoft.com/download/dotnet)

2. **Azure CLI**: Install and configure Azure CLI for accessing Azure Blob Storage.

   You can log in to Azure using the following command, which uses device authentication:

   ```bash
   az login
   ```

3. **User Secrets**: You will need to configure [User Secrets](https://learn.microsoft.com/en-us/aspnet/core/security/app-secrets?view=aspnetcore-8.0&tabs=windows) to securely store your credentials for the TES service and Azure Blob Storage.

   Create a User Secrets file with the following properties:

   - `TesCredentialsPath`: Path to `TesCredentials.json` (created during TES deployment).
   - `StorageAccountName`: The name of your Azure Blob Storage account (used to store output files).

   Example command to configure User Secrets:

   ```bash
   dotnet user-secrets init
   dotnet user-secrets set "TesCredentialsPath" "path/to/TesCredentials.json"
   dotnet user-secrets set "StorageAccountName" "your_storage_account_name"
   ```

## Building the Single File Executable

### For Linux

To package the demo application as a single file executable for Linux, run the following command:

```bash
dotnet publish --configuration Release --output ./publish --self-contained --runtime linux-x64 /p:PublishSingleFile=true
```

### For Windows

To build the demo application as a single file executable for Windows, use this command:

```bash
dotnet publish --configuration Release --output ./publish --self-contained --runtime win-x64 /p:PublishSingleFile=true
```

- Replace `linux-x64` or `win-x64` with your target runtime if you're building for another platform (e.g., `osx-x64` for macOS).
- This command will create a single-file executable in the `./publish` directory.

## Running the Demo

After building the single file executable, you can run the TES SDK examples with the following commands based on the example you want to execute:

1. **Prime Sieve Example**:

   This example submits tasks to calculate prime numbers in a specified range.

   ```bash
   ./Tes.SDK.Examples primesieve [taskCount]
   ```

   - `taskCount`: (Optional) Number of tasks to run. Each task processes a range of 1,000,000 numbers. If omitted, defaults to 1.

   Example:

   ```bash
   ./Tes.SDK.Examples primesieve 10
   ```

2. **BWA Mem Example**:

   This example submits a task to run the BWA Mem algorithm for aligning sequence reads to a reference genome.

   ```bash
   ./Tes.SDK.Examples bwa
   ```

   The outputs will be saved in the specified Azure Blob Storage account.

## Viewing Results

Once the tasks are completed:

- **Prime Sieve**: The output file will be saved in the Azure Blob Storage container `outputs` with the format `primes-{rangeStart}-{rangeEnd}.txt`. You can access the files through the Azure portal or download them locally.
  
- **BWA Mem**: The output BAM file (`H06HDADXX130110.1.ATCACGAT.20k.bam`) will be stored in the `outputs` container of your Azure Blob Storage.

In addition, all output files will be downloaded locally to the temporary directory (`/tmp` or `%TEMP%` on Windows) specified in the script, and the download paths will be logged in the console.
