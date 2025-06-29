#!/bin/bash

source 000_setup.sh

echo "# Download oisst"
./download_code/oisst/download_oisst.sh $rawdata_root

echo "# Postprocessing oisst"
python3 ./download_code/oisst/postprocess_daily.py \
    --input-root $rawdata_root    \
    --output-root $ppdata_root    \
    --date-range 2023-06-10 2023-06-15 \
    --mode daily      

