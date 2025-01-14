import numpy as np
import re

raw_file = "raw_model_version_dates.txt"
output_file = "model_version_dates.txt"

date_fmt = re.compile(r'^\s*(\d{4}-\d{2}-\d{2})\s*$')


with open(output_file, "w") as out_f:
    with open(raw_file, "r") as in_f:
        for s in in_f.readlines():
            m = date_fmt.match(s)
            if m is not None:
                print(m.group(1))
                out_f.write(m.group(1))
                out_f.write("\n")

