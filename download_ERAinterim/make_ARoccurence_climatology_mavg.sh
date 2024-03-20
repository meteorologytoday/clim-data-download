#!/bin/bash

data_dir=data/ERAinterim/AR/24hr
year_beg=1998
year_end=2017


#for method in mean q85 ; do
for method in q85 ; do
    
    ndays=15
    output_dir=climatology_${year_beg}-${year_end}_${ndays}days_ERAInterim_${method}
    mkdir -p $output_dir

    python3 make_AR_climatology_mavg.py \
        --input-dir $data_dir \
        --output-dir $output_dir \
        --input-filename-prefix "ERAInterim-" \
        --output-filename-prefix "ERAInterim-clim-daily_" \
        --year-beg $year_beg \
        --year-end $year_end \
        --mavg-days $ndays  \
        --method "$method" \
        --nproc 1

done

