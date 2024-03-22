#!/bin/bash

## Collect data from the Azure Instance Metadata Service (IMDS)
CURL_TIMEOUT=5

get_imds_data(){
    imds_versions=$(curl --max-time $CURL_TIMEOUT -s -H Metadata:true --noproxy "*" "http://169.254.169.254/metadata/versions")

    # Make sure version 2023-07-01 is available:
    # TODO: As versions are updated this may need to be updated as versions stop being supported
    # TODO: update redaction code as versions are updated
    if [[ $imds_versions == *"2023-07-01"* ]]; then
        version_string="2023-07-01"
    else
        version_string="2021-12-13"
    fi

    # Get instance metadata:
    instance_metadata=$(curl --max-time $CURL_TIMEOUT -s -H Metadata:true --noproxy "*" "http://169.254.169.254/metadata/instance?api-version=$version_string")
    export instance_metadata
}

get_nvme_data(){
    if command -v nvme >/dev/null 2>&1; then
        nvme_metadata=$(sudo nvme list --output-format=json)
        export nvme_metadata
    fi
}

get_imds_data
get_nvme_data

python3 parse_imds_and_nvme_metadata.py
