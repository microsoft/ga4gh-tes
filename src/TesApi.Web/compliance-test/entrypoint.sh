#!/bin/sh
teshostname=$(jq -r '.TesHostname' TesCredentials.json)
tesuser=$(jq -r '.TesUsername' TesCredentials.json)
tespassword=$(jq -r '.TesPassword' TesCredentials.json)

tes-compliance-suite report --server http://$tesuser:$tespassword@teshostname/ --tag all --output_path results