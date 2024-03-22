import sys

class telegraf_output:
    """Take in dictionary or struct data and store it for eventual printing in the telegraf line protocol format.
    If debugging is enabled, output to stderr as well."""
    print_to_stderr = False

    def __init__(self, meter_name="imds_metadata", meter_prefix="imds"):
        self.metrics = {}
        self.tags = {}
        self.meter_name = meter_name
        self.meter_prefix = meter_prefix

    def print_structs(self, data):
        prefix = f"{self.meter_prefix}_" if self.meter_prefix else ""
        for key, val in data.items():
            self.output(prefix + key, val)

    def print_output(self, name, dictionary, keys):
        if isinstance(keys, str):
            keys = [keys]
        value = dictionary
        for key in keys:
            if key in value:
                value = value[key]
            else:
                value = None
                break
        self.output(name, value)

    def output_tag(self, name, value):
        if self.print_to_stderr:
            print(f"TAG:  {name}: '{value}'", file=sys.stderr)
        self.tags[name] = value

    def output(self, name, value):
        if self.print_to_stderr:
            print(f"{name}: '{value}'", file=sys.stderr)
        self.metrics[name] = convert_to_number(value)

    def print_telegraf_output(self):
        # Print the telegraf meter name first
        print(f"{self.meter_name}", end='')
        # Print a comma, then the tags (or just a space if there are no tags)
        if len(self.tags) > 0:
            print(",", end='')
            sep = ""  # Skip the first comma
            for key, val in self.tags.items():
                print(f'{sep}{key}="{val}"', end='')
                sep = ","

        # Print the final space before the metrics:
        print(" ", end='')
        sep = ""  # Skip the first comma
        # Then print every key=value pair
        for key, val in self.metrics.items():
            if type(val) is str:
                print(f'{sep}{key}="{val}"', end='')
            elif type(val) is int:
                print(f'{sep}{key}={val}', end='')
            elif type(val) is float:
                print(f'{sep}{key}={val}', end='')
            sep = ","
        print("")  # Print the newline at the end


def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x
    flatten(y)
    return out


def convert_to_number(value):
    # See if we can convert a string value to a number:
    if type(value) is str:
        try:
            numeric_value = float(value)
            if numeric_value.is_integer():
                numeric_value = int(numeric_value)
            return numeric_value
        except ValueError:
            pass
    return value
