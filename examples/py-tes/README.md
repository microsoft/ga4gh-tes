# py-tes Demostration


## Client requirements

You can install all client dependencies using either [Conda](https://docs.conda.io/projects/conda/en/latest/index.html) or the faster alternative, [Mamba](https://mamba.readthedocs.io/en/latest/) (recommended).

```bash
conda env create -f environment.yml
```

Next, you need to create a file of the TES instance in a
comma-seprated file `.tes_instances`. Two fields/columns are required, a
description of the TES instance, and the URL pointing to it. You can use the
following command to create such a file, but make sure to replace the example
contents and do not use commas in the name/description field:

```bash
cat << "EOF" > .tes_instances
Azure/TES @ YourNode,https://tes.your-node.org/
EOF
```

Finally, you will need to create a secrets file `.env` with the following
command.  You can either set the environment variables in your shell or set the
actual values in the command below.

```bash
cat << EOF > .env
TES_SERVER_USER=$TES_SERVER_USER
TES_SERVER_PASSWORD=$TES_SERVER_PASSWORD
TES_OUTPUT_STORAGE_ACCT=$TES_OUTPUT_STORAGE_ACCT
EOF
```

## Run demo

run the following commands to run BWA example TES Task:

```bash
./run-bwa.py
```