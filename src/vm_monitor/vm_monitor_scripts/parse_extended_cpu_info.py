import subprocess
import json
# from pprint import pprint
# from rich.pretty import pprint
from telegraf_helper import telegraf_output, flatten_json, convert_to_number

# Convert cpuinfo to a dictionary of processors with the processor number as the key
# Remove the 'processor' key from the dictionary and convert values to numbers where possible
def parse_processor_info(processor_info):
    processor = {}
    cpu_id = -1
    for line in processor_info.split('\n'):
        if ':' in line:
            key, value = line.split(':', 1)
            value = convert_to_number(value.strip())
            if key.strip().lower() != "processor":
                processor[key.strip()] = value
            else:
                cpu_id = value
    return cpu_id, processor


# Group like metadata together into groups of processors
# That is, if you have 80 processors all with the same keys print "cpu: 0, 1, 2 .. 80" and their shared values
# instead of 80 rows of the same data
def group_processors_by_keys(processor_dict, keys):
    processor_group = {}
    for processor_id, processor_info in processor_dict.items():
        # Create a new dictionary with just the keys we want to group by
        filtered_info = {key: value for key, value in processor_info.items() if key in keys}
        # Convert the filtered info to a tuple for grouping
        info_tuple = tuple(filtered_info.items())
        # Add the processor to the appropriate group
        processor_group.setdefault(info_tuple, []).append(processor_id)
    # At the moment we have a dict of tuples with processor_ids as values
    # convert to a dict of string processor ids with the processor info as the value
    processor_group = {'_'.join([str(i) for i in v]):dict(k) for k,v in processor_group.items()}
    # Cleanup the field names inside each processor group:
    for cpu_group in processor_group.keys():
        processor_group[cpu_group] = {k.replace(' ', '_').replace('(','').replace(')','').replace(':',''):v for k,v in processor_group[cpu_group].items()}
    return processor_group


# Group all keys except the ones which vary in every processor
def group_processors_by_key_values(processor_dict):
    # Get the keys from the first processor info dictionary
    first_processor_info = next(iter(processor_dict.values()))
    keys = set(first_processor_info.keys()) - {'processor', 'bogomips', 'initial apicid', 'apicid', 'core id'}
    return group_processors_by_keys(processor_dict, keys)


def group_processors_by_core_architecture_key_values(processor_dict):
    keys = {'cpu cores', 'physical id', 'siblings', 'core id'}
    return group_processors_by_keys(processor_dict, keys)


def print_telegraf_processor_info(grouped_processor_info, meter_name, prefix=""):
    for cpu, values in grouped_processor_info.items():
        results = telegraf_output(meter_name, prefix)
        results.output_tag("cpu", cpu)
        results.print_structs(flatten_json(values))
        results.print_telegraf_output()


def parse_cpuinfo():
    # Process the /proc/cpuinfo data:
    command = "cat /proc/cpuinfo"
    cpu_info = subprocess.check_output(command, shell=True).decode().strip()
    processor_list = cpu_info.split('\n\n')
    # Create a dictionary (key as processor number and value as the processor info)
    processors = {k:v for k,v in (parse_processor_info(x) for x in processor_list)}
    try:
        # Create a few different views into the processor data:
        processor_info = group_processors_by_key_values(processors)
        processor_architecture_info = group_processors_by_core_architecture_key_values(processors)
        cpu_perf_info = {
            processor_id: {
                'bogomips':processor_info['bogomips'],
                'cpu_MHz':processor_info['cpu MHz'] if 'cpu MHz' in processor_info else None,
                } for processor_id, processor_info in processors.items()}

        # Output to telegraf
        print_telegraf_processor_info(processor_info, "vm_cpuinfo_metadata")
        print_telegraf_processor_info(processor_architecture_info, "vm_cpuinfo_arch_metadata")
        print_telegraf_processor_info(cpu_perf_info, "vm_cpuinfo_perf_metadata")
    except:
        result = telegraf_output("vm_cpuinfo_metadata", "")
        result.output("parser_result", "error_processing")
        result.print_telegraf_output()


def parse_lscpu_info():
    # Process the lscpu output (lscpu is likely easier to use than /proc/cpuinfo):
    command = "lscpu -J"
    lscpu_info = subprocess.check_output(command, shell=True).decode().strip()
    # Convert the lscpu output to a dictionary
    lscpu = json.loads(lscpu_info)
    meter_name = "vm_lscpu_metadata"
    meter_prefix = ""
    results = telegraf_output(meter_name, meter_prefix)
    try:
        lscpu_dict = {x['field']:x['data'] for x in lscpu['lscpu']}
        # Cleanup the field names:
        lscpu_dict = {k.replace(' ', '_').replace('(','').replace(')','').replace(':',''):v for k,v in lscpu_dict.items()}
        lscpu_dict = {k:convert_to_number(v) for k,v in lscpu_dict.items()}
        results.print_structs(lscpu_dict)
        results.output("parser_result", "true")
    except:
        results.output("parser_result", "error_processing")
    results.print_telegraf_output()


def main():
    parse_cpuinfo()
    parse_lscpu_info()


if __name__ == '__main__':
    main()
