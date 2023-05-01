# TES Node Runner

## Overview

The TES Node Runner is an executable that handles the execution of  a TES task on a compute node.
The TES Task consist of the following operations:

- Inputs download: Input files to be downloaded (from any HTTP source).
- Execution of commands: Command(s) to execute on a Docker container 
- Outputs upload: Output files to be uploaded to Azure Storage.

## TES Node Runner CLI

The TES Node Runner is a command line executable that takes the following arguments:

```bash
Usage:
  tRunner [command] [options]

Options:
  --version       Show version information
  -?, -h, --help  Show help and usage information

Commands:
  download  Downloads input files from a HTTP source
  upload    Uploads output files to blob storage
  exec      Downloads input files, executes specified commands and uploads output files
```

## TES Task Definition

The operations are defined in a TES task file using ``yml`` 

```yaml
ImageTag: latest
ImageName: busybox
CommandsToExecute:
- echo
- Hello Docker!
Inputs:
- FullFileName: <LOCAL_PATH> 
  SourceUrl: <SOURCE_URL>
  SasStrategy: None
Outputs:
- FullFileName: <LOCAL_PATH>
  TargetUrl: <TARGET_URL>
  SasStrategy: None

```


## Download and Upload

The download and upload operations are optimized for performance and large files. These operations are modeled as a pipeline with multiple readers and writers of parts (blocks). This translates into an efficient transfer of multiple files with as much concurrency as possible given the node's resources. There are several knobs that can be tuned to optimize these operations. These are exposed as additional options in the `upload` and `download` commands. 

```bash
Usage:
  tRunner download | upload [options]

Options:
  --file <file>              The file with the task definition.
  --blockSize <blockSize>    Blob block size. [default: 10485760]
  --writers <writers>        Number of concurrent writers [default: 10]
  --readers <readers>        Number of concurrent readers [default: 10]
  --apiVersion <apiVersion>  Azure Storage API version [default: 2020-10-02]
  -?, -h, --help             Show help and usage information
```

## SAS Token Resolution Strategy 

Downloads and uploads currently support only SAS tokens or public endpoints. For SAS tokens, the implementation has an extensibility framework to resolve the SAS tokens using different approaches (strategies). 

Strategies are implementations of:

```c#
public interface ISasResolutionStrategy
    {
        Task<Uri> CreateSasTokenWithStrategyAsync(string sourceUrl);
    }
```

For each input or output, an strategy implementation can be specified. 

The list of supported strategies are:

**TODO**


