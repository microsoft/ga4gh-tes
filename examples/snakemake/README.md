# Snakemake DemoDemonstraion

This guide will help you set up **Snakemake** and configure it to run with a **Task Execution Service (TES)**. Follow the steps below to install Snakemake, set up the environment, and configure TES for running workflows.

## Client requirements

You can install all client dependencies using either [Conda](https://docs.conda.io/projects/conda/en/latest/index.html) or the faster alternative, [Mamba](https://mamba.readthedocs.io/en/latest/) (recommended).

```bash
conda env create -f environment.yml
```

## Create TES Configuration

You will need to set the environment variables in your shell.

```bash
export TES_ENDPOINT="<Your TES endpoint>"
export TES_USERNAME="<Your TES username>"
export TES_PASSWORD="<Your TES password>"
export STORAGE_ACCT="<Your Azure Storage account password>"
export AZURITE_CONNECTION_STRING="<Your Azure Storage Connection String>"
```

You will need to update the `Snakefile` with the name of the storage account.


## Running a Snakemake Workflow with TES

Now that Snakemake is installed and TES is configured, you can run your Snakemake workflow on TES.

### Example Snakemake Command

Run the workflow with TES as the executor:

```bash
snakemake --executor tes --tes-url $TES_ENDPOINT --tes-user $TES_USERNAME$ --tes-password $TES_PASSWORD --default-storage-provider azure --default-storage-prefix 'az://inputs/snakemake'  --storage-azure-account-name  $STORAGE_ACCT -j1 --envvars AZURITE_CONNECTION_STRING --use-conda --verbose 
```

## Step 4: Viewing Results

Once the workflow is complete, Azure Blob Storage container `outputs/crankshaft-tes`. You can access these files through the Azure portal or your command-line tool of choice.

## Additional Resources
- [Snakemake TES Documentation](https://snakemake.github.io/snakemake-plugin-catalog/plugins/executor/tes.html)
- [Snakemake Azure Documentation](https://snakemake.github.io/snakemake-plugin-catalog/plugins/storage/azure.html)

