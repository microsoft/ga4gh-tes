#!/bin/bash

cd "$(dirname "$0")" || exit 1

# Expected to be called from Telegraf's exec plugin
# Run and collect all telegraf stats, not all of this information will be useful but it is collected for
# completeness and debugging purposes.
./get_imds_and_nvme_metatada.sh || true
./get_linux_boot_iso_timestamp.sh || true
python3 parse_extended_cpu_info.py || true
# Called last in case batch agent has changed
./get_batch_agent_values.sh || true

