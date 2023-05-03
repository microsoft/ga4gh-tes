# TES Node Runner

## Overview

The TES Node Runner is an executable that handles the execution of a TES task on a compute node.

The TES task consists of the following operations:

- Inputs download Input files to be downloaded (from any HTTP source).
- Execution of commands: Command(s) to execute on a Docker container 
- Outputs upload Output files to be uploaded to Azure Storage.

## TES Node Runner CLI

The TES Node Runner is a command line executable that takes several arguments. Its root command is the execution of the TES task.

The following command executes the TES task in the default location - the same directory as the executable. 

```bash
./tRunner
```

There are several options that can be provided:

```bash

Options:
  -f, --file <file> (REQUIRED)           The file with the task definition [default: TesTask.json]
  -b, --blockSize <blockSize>            Blob block size in bytes [default: 10485760]
  -w, --writers <writers>                Number of concurrent writers [default: 10]
  -r, --readers <readers>                Number of concurrent readers [default: 10]
  -c, --bufferCapacity <bufferCapacity>  Pipeline buffer capacity [default: 10]
  -v, --apiVersion <apiVersion>          Azure Storage API version [default: 2020-10-02]
  -u, --docker-url <docker-url>          local docker engine endpoint [default:
                                         unix:///var/run/docker.sock]
  --version                              Show version information
  -?, -h, --help                         Show help and usage information
```

## TES Task Definition

The operations are defined in a TES task file using ``JSON`` 

```json
{
    "imageTag": "latest",
    "imageName": "busybox",
    "commandsToExecute": [
        "echo",
        "Hello Docker!"
    ],
    "inputs": [
        {
            "fullFileName": "<LOCAL_PATH>",
            "sourceUrl": "<SOURCE_URL>",
            "sasStrategy": "None"
        }
    ],
    "outputs": [
        {
            "fullFileName": "<LOCAL_PATH>",
            "targetUrl": "<TARGET_URL>",
            "sasStrategy": "None"
        }
    ]
}
```


## Download and Upload

The download and upload operations are optimized for performance and large files. These operations are modeled as a pipeline with multiple readers and writers of parts (blocks). This translates into an efficient transfer of multiple files with as much concurrency as possible given the node's resources. There are several knobs that can be tuned to optimize these operations. These operations can be executed in isolation using the `upload` and `download` commands. 

```bash
Usage:
  tRunner download | upload [options]

Options:
  -f, --file <file> (REQUIRED)           The file with the task definition [default: TesTask.json]
  -b, --blockSize <blockSize>            Blob block size in bytes [default: 10485760]
  -w, --writers <writers>                Number of concurrent writers [default: 10]
  -r, --readers <readers>                Number of concurrent readers [default: 10]
  -c, --bufferCapacity <bufferCapacity>  Pipeline buffer capacity [default: 10]
  -v, --apiVersion <apiVersion>          Azure Storage API version [default: 2020-10-02]
  -f, --file <file> (REQUIRED)           The file with the task definition [default: TesTask.json]
  -b, --blockSize <blockSize>            Blob block size in bytes [default: 10485760]
  -w, --writers <writers>                Number of concurrent writers [default: 10]
  -r, --readers <readers>                Number of concurrent readers [default: 10]
  -c, --bufferCapacity <bufferCapacity>  Pipeline buffer capacity [default: 10]
  -v, --apiVersion <apiVersion>          Azure Storage API version [default: 2020-10-02]
  -?, -h, --help                         Show help and usage information
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


