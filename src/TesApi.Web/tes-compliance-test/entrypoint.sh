#!/bin/sh
teshostname=$(jq -r '.TesHostname' TesCredentials.json)
tesuser=$(jq -r '.TesUsername' TesCredentials.json)
tespassword=$(jq -r '.TesPassword' TesCredentials.json)

# This file will be used to run tes compliance tests as part of the ADO release
# openapi-test-runner repo provides a default entry point script, but since TES uses basic auth it needs to be replaced to add approprite credentials
# https://github.com/elixir-cloud-aai/openapi-test-runner
openapi-test-runner report --server http://$tesuser:$tespassword@$teshostname/ --version 1.0.0 --output_path results
mv results/report.json results/report-tes-1.0.0.json
openapi-test-runner report --server http://$tesuser:$tespassword@$teshostname/ --version 1.1.0 --output_path results
mv results/report.json results/report-tes-1.1.0.json
jq -n 'reduce inputs as $s (.; .[input_filename] += $s)'   results/report-tes-1.0.0.json results/report-tes-1.1.0.json > results/report.json
