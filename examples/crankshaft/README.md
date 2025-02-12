# St. Jude TES Demonstration

Demostration of use of `tes` in your projects from St. Jude


## Client requirements

To use `tes`, you'll need to install [Rust](https://www.rust-lang.org/).
We recommend using [rustup](https://rustup.rs/) to accomplish this. 


To utilize `tes` in your crates, simply add it to your project.

```bash
# If you want to use the types.
cargo add tes

# If you also want to use the provided client.
cargo add tes --features client,serde
```

You will need to set the environment variables in your shell.
```bash
export STORAGE_ACCT=<STORAGE_ACCT>
export USER=<TES_USERNAME>
export PASSWORD=<TES_PASSWORD>
```

For this demo we have created two projects `tes_example` 
for you to use.

## Run TES demo

run the following commands to run BWA example TES Task:

```bash
cd tes_example
cargo build
cargo run $URL
```


## Viewing Results

After the pipeline completes, all results will be saved in the Azure Blob Storage container `outputs/cracrankshaft-tes`. You can access these files through the Azure portal or your command-line tool of choice.
