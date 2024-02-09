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
./tes-runner
```

There are several options that can be provided:

```bash

Options:
  -f, --file <file> (REQUIRED)           The file with the task definition [default: runner-task.json]
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
            "path": "<PATH>",
            "sourceUrl": "<SOURCE_URL>",
            "mountParentDirectory" : "<MOUNT_PARENT_DIRECTORY>",
            "sasStrategy": "None",
        }
    ],
    "outputs": [
        {
            "path": "<PATH>",
            "targetUrl": "<TARGET_URL>",
            "sasStrategy": "None",
            "mountParentDirectory" : "<MOUNT_PARENT_DIRECTORY>",
            "fileType": "<FILE_TYPE>"
        }
    ]
    "runtimeOptions": {
        "terraRuntimeOptions": {
            "wsmApiHost":"<WSM_API_HOST>",
            "landingZoneApiHost":"<LZ_API_HOST>",
            "sasAllowedIpRange":""
        }
    }
}
```

The following table describes the fields in the TES task file:

| Field | Description | Required |
| --- | --- | --- |
| `imageTag` | The tag of the Docker image to use | Yes, only for default command (execution) |
| `imageName` | The name of the Docker image to use | Yes, only for default command (execution) |
| `commandsToExecute` | The list of commands to execute | Yes, only for default command (execution) |
| `inputs` | The list of input files to download | No |
| `outputs` | The list of output files to upload | No |
| `runtimeOptions` | TES node runner runtime options | Yes, the structure is defined below |

### Inputs

The inputs are defined as a list of objects with the following fields:

| Field | Description | Required |
| --- | --- | --- |
| `path` | The local absolute path of the input file.  | Yes |
| `sourceUrl` | The URL of the input file | Yes |
| `sasStrategy` | The strategy to resolve the SAS token | Yes |
| `mountParentDirectory` | The directory from which the children directories must be mapped as a volume in the Docker container | No |

### Outputs

The outputs are defined as a list of objects with the following fields:

| Field | Description | Required |
| --- | --- | --- |
| `path` | The local absolute path of the output file, directory or the search pattern | Yes |
| `targetUrl` | The URL of the output file | Yes |
| `sasStrategy` | The strategy to resolve the SAS token | Yes |
| `fileType` | `File` or `Directory`. If the value is `Directory` value in the `path` property must be a directory. All files in the directory structure are uploaded. | Yes |
| `mountParentDirectory` | The directory from which the children directories must be mapped as a volume in the Docker container | No |

## Download and Upload

The download and upload operations are optimized for performance and large files. These operations are modeled as a pipeline with multiple readers and writers of parts (blocks). This translates into an efficient transfer of multiple files with as much concurrency as possible given the node's resources. There are several knobs that can be tuned to optimize these operations. These operations can be executed in isolation using the `upload` and `download` commands. 

```bash
Usage:
 tes-runner download | upload [options]

Options:
  -f, --file <file> (REQUIRED)           The file with the task definition [default: runner-task.json]
  -b, --blockSize <blockSize>            Blob block size in bytes [default: 10485760]
  -w, --writers <writers>                Number of concurrent writers [default: 10]
  -r, --readers <readers>                Number of concurrent readers [default: 10]
  -c, --bufferCapacity <bufferCapacity>  Pipeline buffer capacity [default: 10]
  -v, --apiVersion <apiVersion>          Azure Storage API version [default: 2020-10-02]
  -f, --file <file> (REQUIRED)           The file with the task definition [default: runner-task.json]
  -b, --blockSize <blockSize>            Blob block size in bytes [default: 10485760]
  -w, --writers <writers>                Number of concurrent writers [default: 10]
  -r, --readers <readers>                Number of concurrent readers [default: 10]
  -c, --bufferCapacity <bufferCapacity>  Pipeline buffer capacity [default: 10]
  -v, --apiVersion <apiVersion>          Azure Storage API version [default: 2020-10-02]
  -?, -h, --help                         Show help and usage information
```

## URL Transformation Strategies

The TES node runner supports different strategies to transform the URLs for inputs and output files. These transformations are required so the runner can download or upload files as required. 

A transformation strategy is a way to resolve the SAS token for the input or output URL or convert a URI using an non HTTP scheme (e.g. s3://) to a valid HTTP URL.

Strategies are implementations of:

```c#
public interface IUrlTransformationStrategy
    {
        Task<Uri> TransformUrlWithStrategyAsync(string sourceUrl);
    }
```

For each input or output, an strategy implementation can be specified. 

The list of supported strategies are:

| Strategy | Description |
| --- | --- |
| `None` | No transformation is applied. The URL is used as is. |
| `AzureResourceManager` | Generates a SAS token using the a user delegated key. Only applies if the URL provided is a Blob endpoint with the suffix: .blob.core.windows.net. |
| `TerraWsm` | Generates a SAS token using Terra WSM. Only applies if the URL provided is a managed Terra storage account |
| `SchemeConverter` | Converts URIs with the following schemes: `s3://` or `gs://` to valid HTTP URLs.|
| `CombinedTerra` | Applies the `SchemaConverter` strategy and `TerraWsm`.|
| `CombinedAzureResourceManager` | Applies the `SchemaConverter` strategy and `AzureResourceManager`.|
        


### Runtime Options

The TES node runner has several runtime options that can be configured in the TES task file. 

```json
"runtimeOptions": {
        "terraRuntimeOptions": {
            "wsmApiHost":"<WSM_API_HOST>",
            "landingZoneApiHost":"<LZ_API_HOST>",
            "sasAllowedIpRange":""
        }
    }
```

The Terra runtime options are defined in the `runtimeOptions` property of the TES task file. 
These are only required when running on Terra.

| Terra Runtime Option | Description | Required |
| --- | --- | --- |
| `wsmApiHost` | The WSM API host | Yes |
| `landingZoneApiHost` | The Landing Zone API host | Yes |
| `sasAllowedIpRange` | The allowed IP range for SAS token resolution | No |

