import sys
import json
import re
import string

# This script uses json.dumps to serialize strings with complex quoting/escaping issues into
# a single line string that can be placed inside double quotes
input_log_line = sys.stdin.read()
# Remove non-printable characters (such as the block character in agent logs)
clean_line = re.sub(f'[^{re.escape(string.printable)}]', '_', input_log_line)
clean_line = json.dumps(clean_line)
clean_line = clean_line.replace("\\n",'')
sys.stdout.write(clean_line.strip())