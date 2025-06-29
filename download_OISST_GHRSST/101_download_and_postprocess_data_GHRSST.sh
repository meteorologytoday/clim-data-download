#!/bin/bash

source 000_setup.sh

raw_data_root=$data_dir
postprocessed_data_root=$data_dir
start_date=2023-06-10T00:00:00Z
end_date=2023-06-15T00:00:00Z

if [ ] ; then
echo "# Download GHRSST"
./download_code/GHRSST/download.sh $rawdata_root $start_date $end_date
fi

echo "# Postprocessing GHRSST"

for dataset in OSTIA_UKMO ; do

    python3 ./download_code/GHRSST/postprocess_daily.py \
        --input-root $rawdata_root    \
        --output-root $ppdata_root    \
        --date-range $start_date $end_date \
        --datasets $dataset \
        --mode daily     

done 
