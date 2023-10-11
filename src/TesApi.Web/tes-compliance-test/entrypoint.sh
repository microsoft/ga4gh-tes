#!/bin/sh
teshostname=$(jq -r '.TesHostname' TesCredentials.json)
tesuser=$(jq -r '.TesUsername' TesCredentials.json)
tespassword=$(jq -r '.TesPassword' TesCredentials.json)

# This file will be used to run tes-compliance-tests as part of the ADO release
# TES-compliance-suite repo provides a default entry point script, but since TES uses basic auth it needs to be replaced to add approprite credentials
# https://github.com/elixir-cloud-aai/tes-compliance-suite
openapi-test-runner report --server http://$tesuser:$tespassword@$teshostname/ --tag all --version 1.0.0 --output_path results
