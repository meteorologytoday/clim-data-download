#!/bin/bash

input_dir=/data/SO2/t2hsu/clim-data-download/download_ERA5/climatology_1998-2017_15days_ERAInterim_q85
output_dir=./ECCC_data/climatology_1998-2017_15days_ERAInterim_q85-interpolatedECCCgrid
ref_latlon=./ECCC_data/data20/postprocessed/GEPS5/AR/ECCC-S2S_GEPS5_AR_2013_01-31.nc
filename_prefix=ERAInterim-clim-daily_


echo "Run interpolation program"
python3 interpolate_ERA-interim_data.py \
    --input-dir $input_dir \
    --output-dir $output_dir \
    --output-latlon $ref_latlon \
    --filename-prefix $filename_prefix \
    --nproc 1
