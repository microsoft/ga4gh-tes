# Azure Append Blob Output Plugin

This plugin writes telegraf metrics to an append blob on an Azure Storage Account.
Plugin was developed using the File Output plugin as a base with additions from
the Azure Monitor output. Note that test does not work and the plugin is very
specific to the GA4GH TES monitoring task it is being used for. This plugin
would require significant time investment to make it an official plugin.

Note that output

## Global configuration options <!-- @/docs/includes/plugin_config.md -->

In addition to the plugin-specific configuration settings, plugins support
additional global and plugin configuration settings. These settings are used to
modify metrics, tags, and field or create aliases and configure ordering, etc.
See the [CONFIGURATION.md][CONFIGURATION.md] for more details.

[CONFIGURATION.md]: ../../../docs/CONFIGURATION.md#plugins

## Configuration

```toml @sample.conf
# Send metrics to an Azure Storage Account using an append blob
[[outputs.azure_append_blob]]
  ## Azure Storage Account destination is specified in 4 parts, the storage account,
  ## the azure_endpoint (optional), the container name, and the path to where the blobs
  ## will be written. By default this plugin assumes it will be writing files called
  ## "vm_metrics.%d.json". So output_path should be a directory.
  storage_account_name = "myStorageAccountName"
  container_name = "data"
  output_path = "/workflow/task_name/iteration/"

  ## Use batch serialization format instead of line based delimiting.  The
  ## batch format allows for the production of non line based output formats and
  ## may more efficiently encode and write metrics.
  # use_batch_format = false

  ## Data format to output.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md
  data_format = "influx"

  ## Compress output data with the specified algorithm.
  ## If empty, compression will be disabled and files will be plain text.
  ## Supported algorithms are "zstd", "gzip" and "zlib".
  # compression_algorithm = ""

  ## Compression level for the algorithm above.
  ## Please note that different algorithms support different levels:
  ##   zstd  -- supports levels 1, 3, 7 and 11.
  ##   gzip -- supports levels 0, 1 and 9.
  ##   zlib -- supports levels 0, 1, and 9.
  ## By default the default compression level for each algorithm is used.
  # compression_level = -1

  ## Optionally, if in Azure US Government, China, or other sovereign
  ## cloud environment, set the appropriate endpoint
  # azure_endpoint = "blob.core.usgovcloudapi.net"
```
