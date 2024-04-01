#!/bin/bash

data_dir=data/ERAinterim/ARObjects/HMGFSC24_threshold-1998-2017
year_beg=1998
year_end=2017


ndays=15
output_dir=data/ERAinterim/stat/ARoccurence/${year_beg}-${year_end}_mavg_${ndays}days
mkdir -p $output_dir

python3 make_ARoccurence_climatology_mavg.py \
    --input-dir $data_dir \
    --output-dir $output_dir \
    --input-filename-prefix "ARobjs_" \
    --output-filename-prefix "ERAInterim-clim-daily_" \
    --year-beg $year_beg \
    --year-end $year_end \
    --mavg-days $ndays  \
    --nproc 5

