import os
import sys
import json
from datetime import datetime
# For better debugging (rich must be installed)
# from rich.pretty import pprint

from telegraf_helper import telegraf_output, flatten_json

def prune_imds_data(imds_data):
    redacted_string = "<REDACTED>"
    # Recursively walk every string, redact any secrets
    # (this is a weak-redaction and deletes things like 'disablePasswordAuthentication: false', but it's better than nothing)
    #  - string values starting with 'ssh-rsa'
    #  - string values with 'sv=' and 'sig=' (Azure SAS tokens)
    #  - keys that contain 'secret', 'password', or 'key'
    def redact_secrets(data):
        if type(data) is str:
            if data.startswith('ssh-rsa'):
                data = redacted_string
            if 'sv=' in data and 'sig=' in data:
                data = redacted_string
        elif type(data) is dict:
            for k, v in data.items():
                if any(word in k.lower() for word in ['secret', 'password', 'key']):
                    data[k] = redacted_string
                else:
                    data[k] = redact_secrets(v)
        elif type(data) is list:
            for i, v in enumerate(data):
                data[i] = redact_secrets(v)
        return data
    def key_exists(data, key):
        for k in key:
            if k not in data:
                return False
            data = data[k]
        return True
    # Explicitly drop keys that shouldn't be recorded:
    if key_exists(imds_data, ['compute', 'publicKeys']):
        imds_data['compute']['publicKeys'] = redacted_string
    return redact_secrets(imds_data)


def parse_imds_metadata():
    results = telegraf_output('imds_metadata','imds')
    try:
        instance_metadata = os.getenv('instance_metadata')
        if not instance_metadata:
            results.output("parser_result", "no_data")
            results.print_telegraf_output()
            return
        imds_data = json.loads(instance_metadata)
        # Drop keys that shouldn't be recorded:
        imds_data = prune_imds_data(imds_data)
        compute = imds_data['compute']
        results.print_output("vm_tags", compute, 'tags')
        results.print_output("vm_id", compute, 'vmId')
        results.print_output("vm_size", compute, 'vmSize')
        results.print_output("zone", compute, 'zone')
        results.print_output("vm_location", compute, 'location')
        results.print_output("vm_priority", compute, 'priority')
        results.print_output("vm_resource_id", compute, 'resourceId')
        results.print_output("vm_encryption_at_host", compute, ['securityProfile', 'encryptionAtHost'])
        # Print individual important tags:
        if 'tagsList' in compute:
            tagsList = compute['tagsList']
            # convert "name"/"value" pairs to a dictionary
            tagsList = {x['name']: x['value'] for x in tagsList}
            results.print_output("batch_account_name", tagsList, 'BatchAccountName')
            results.print_output("batch_subscription_id", tagsList, 'BatchAccountSubscription')
            results.print_output("batch_pool_name_fq", tagsList, 'FQPoolName')
            results.print_output("batch_pool_name", tagsList, 'PoolName')
            results.print_output("vm_low_priority_type", tagsList, 'LowPriorityType')
        # Print storage data:
        if 'storageProfile' in compute:
            storage = compute['storageProfile']
            results.print_output("vm_resource_disk_size", storage, ['resourceDisk', 'size'])
            results.print_output("vm_image_offer", storage, ['imageReference', 'offer'])
            results.print_output("vm_image_sku", storage, ['imageReference', 'sku'])
            if 'osDisk' in storage:
                osdisk = storage['osDisk']
                results.print_output("vm_os_disk_size", osdisk, 'diskSizeGB')
                results.print_output("vm_os_disk_caching", osdisk, 'caching')
                results.print_output("vm_managed_disk_type", osdisk, ['managedDisk', 'storageAccountType'])
                results.print_output("vm_os_disk_write_accelerator_enabled", osdisk, 'writeAcceleratorEnabled')
        # Print network interfaces data:
        if 'network' in imds_data:
            results.print_structs(flatten_json(imds_data['network']))
        results.output("parser_result", "true")
        # Print the imds JSON struct for debugging purposes
        print(f'debug_instance_metadata_json data={json.dumps(json.dumps(imds_data))}')
    except:
        results.output("parser_result", "error_processing")
    results.print_telegraf_output()


def parse_nvme_metadata():
    def nvme_error_result(msg):
        results = telegraf_output('nvme_metadata','vm_nvme')
        results.output("parser_result", msg)
        results.print_telegraf_output()
    try:
        nvme_metadata = os.getenv('nvme_metadata')
        if not nvme_metadata:
            nvme_error_result("no_data")
            return
        nvmedata = json.loads(nvme_metadata)
        # For every nvme disk print by index their stats:
        if 'Devices' not in nvmedata:
            nvme_error_result("error_processing")
            return
        for dev in nvmedata['Devices']:
            results = telegraf_output('nvme_metadata','vm_nvme')
            results.output("parser_result", "true")
            results.output_tag("nvme_disk_index", dev['Index'])
            results.output("name", dev['DevicePath'])
            results.output("used_bytes", dev['UsedBytes'])
            results.output("max_lba", dev['MaximumLBA'])
            results.output("physical_size", dev['PhysicalSize'])
            results.output("sector_size", dev['SectorSize'])
            results.print_telegraf_output()
        # Print the nvme_metadata JSON struct for debugging purposes
        print(f'debug_nvme_metadata_json data={json.dumps(json.dumps(nvmedata))}')
    except:
        nvme_error_result("error_processing")


def main():
    # Init an object to hold stats output so we can have a single line output for all stats
    # This meter is called 'imds_metadata'
    parse_imds_metadata()
    parse_nvme_metadata()


if __name__ == '__main__':
    main()
