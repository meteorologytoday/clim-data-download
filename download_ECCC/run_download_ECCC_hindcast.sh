#!/bin/bash

archive_root=./ECCC_data/data_test






python3 download_ECCC_forecast.py \
    --nwp-type hindcast \
    --archive-root $archive_root \
    --date-range 2015-01-01 2016-01-01 \
    --model-versions "GEPS6" \
    --hindcast-year-range 2015 2016 


