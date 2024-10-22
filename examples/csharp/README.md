# TES C# SDK Demonstration

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

## Running the Demo

To run the TES SDK examples, execute the following commands based on which example you want to run:

1. **Prime Sieve Example**:

   This example submits tasks to calculate prime numbers in a specified range.

   ```bash
   dotnet run --project TesExamples -- primesieve [taskCount]
   ```

   - `taskCount`: (Optional) Number of tasks to run. Each task processes a range of 1,000,000 numbers. If omitted, defaults to 1.

   Example:
   ```bash
   dotnet run --project TesExamples -- primesieve 10
   ```

2. **BWA Mem Example**:

   This example submits a task to run the BWA Mem algorithm for aligning sequence reads to a reference genome.

   ```bash
   dotnet run --project TesExamples -- bwa
   ```

   The outputs will be saved in the specified Azure Blob Storage account.

## Viewing Results

Once the tasks are completed:

- **Prime Sieve**: The output file will be saved in the Azure Blob Storage container `outputs` with the format `primes-{rangeStart}-{rangeEnd}.txt`. You can access the files through the Azure portal or download them locally.
  
- **BWA Mem**: The output BAM file (`H06HDADXX130110.1.ATCACGAT.20k.bam`) will be stored in the `outputs` container of your Azure Blob Storage.

In addition, all output files will be downloaded locally to the temporary directory (`/tmp`) specified in the script, and the download paths will be logged in the console.
