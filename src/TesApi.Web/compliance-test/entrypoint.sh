#!/bin/sh

ls
cd /app/tes-compliance-suite/

tes-compliance-suite report --server https://localhost/ --tag all --output_path /app/results