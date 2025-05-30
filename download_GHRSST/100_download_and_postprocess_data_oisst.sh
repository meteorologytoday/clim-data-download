#!/bin/bash

source 000_setup.sh


raw_data_dir=$data_dir/physical/sst_raw/oisst
postprocessed_data_dir=$data_dir

echo "# Download oisst"
./src/data_download/oisst/download_oisst.sh $raw_data_dir

echo "# Postprocessing oisst"
python3 ./src/data_download/oisst/postprocess.py \
    --input-root $raw_data_dir              \
    --output-root $postprocessed_data_dir   \
    --year-rng 2007 2024      

