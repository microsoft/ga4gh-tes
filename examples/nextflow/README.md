# Nextflow Demonstration

## Client requirements

Make sure you install [nextflow](https://www.nextflow.io/) before you get started


Nextflow requires Java (version 8 or 11) and can be installed on Linux, macOS, or Windows.

### Installation Instructions

#### Linux / macOS:
1. Open your terminal and run the following commands:
   ```bash
   curl -s https://get.nextflow.io | bash
   ```
2. Move the Nextflow binary to a directory in your `PATH`, for example:
   ```bash
   mv nextflow /usr/local/bin/
   ```

#### Windows:
1. Install Windows Subsystem for Linux (WSL) if you are on Windows.
2. Follow the Linux installation steps once WSL is set up.

For more details, visit the official [Nextflow installation guide](https://www.nextflow.io/docs/latest/getstarted.html).

## Generate the TES Config

Create a configuration file named `tes.config` to connect Nextflow with TES and your Azure storage. Fill in your TES and Azure credentials as shown below:

```groovy
process {
  executor = 'tes'
}

azure {
  storage {
    accountName = "<Your storage account name>"
    accountKey  = "<Your storage account key>"
  }
}

tes.endpoint       = "<Your TES endpoint>"
tes.basicUsername  = "<Your TES username>"
tes.basicPassword  = "<Your TES password>"
```

## Run demo

To help you get up and running quickly, we’re introducing the [`nf-hello-gatk`](https://github.com/seqeralabs/nf-hello-gatk/tree/main) project, a Nextflow pipeline example designed to showcase the powerful capabilities of Nextflow.


```bash
./nextflow run seqeralabs/nf-hello-gatk -c tes.config -w 'az://work' --outdir 'az://outputs/nextflow' -r main
```

### Explanation:
- `-c tes.config`: Specifies the TES configuration file with your credentials.
- `-w 'az://work'`: Azure Blob Storage container for intermediate workflow files.
- `--outdir 'az://outputs/nextflow'`: Specifies the output directory in your Azure Blob Storage.
- `-r main`: Runs the main branch of the pipeline repository.

## Viewing Results

After the pipeline completes, all results will be saved in the Azure Blob Storage container specified by `--outdir`. You can access these files through the Azure portal or your command-line tool of choice.

