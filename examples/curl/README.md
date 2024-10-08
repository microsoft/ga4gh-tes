# Curl Demonstration


## Client requirements


Make sure you install `jq` if not already present. `jq` is a lightweight and flexible command-line JSON processor.


Next, you need to create a file of the TES instance in a
comma-separated file `.tes_instances`. Two fields/columns are required, a
name of the TES instance, and the URL pointing to it. You can use the
following command to create such a file, but make sure to replace the example
contents and do not use commas in the name/description field:

```bash
cat << "EOF" > .tes_instances
Azure/TES,https://tes.your-node.org
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
./run-bwa.bash
```

## Viewing Results

After the pipeline completes, all results will be saved in the Azure Blob Storage container `outputs/curl`. You can access these files through the Azure portal or your command-line tool of choice.
