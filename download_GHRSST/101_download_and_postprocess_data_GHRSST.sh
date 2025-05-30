#!/bin/bash

source 000_setup.sh

raw_data_root=$data_dir
postprocessed_data_root=$data_dir

echo "# Download GHRSST"
./src/data_download/GHRSST-group/download_GHRSST.sh $raw_data_root


echo "# Postprocessing GHRSST"
python3 ./src/data_download/GHRSST-group/postprocess.py \
    --input-root $raw_data_root             \
    --output-root $postprocessed_data_root  \
    --year-rng 2023 2023 

