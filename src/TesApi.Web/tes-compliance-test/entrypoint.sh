#!/bin/sh
teshostname=$(jq -r '.TesHostname' TesCredentials.json)
tesuser=$(jq -r '.TesUsername' TesCredentials.json)
tespassword=$(jq -r '.TesPassword' TesCredentials.json)

# This file will be used to run tes-compliance-tests as part of the ADO release
# TES-compliance-suite repo provides a default entry point script, but since TES uses basic auth it needs to be replaced to add approprite credentials
# https://github.com/elixir-cloud-aai/tes-compliance-suite
tes-compliance-suite report --server http://$tesuser:$tespassword@$teshostname/ --tag all --output_path results