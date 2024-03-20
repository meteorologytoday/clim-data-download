#!/bin/bash

data_dir=ECCC_data/data20
clim_data_dir=ECCC_data/climatology_1998-2017_15days_ERAInterim_q85-interpolatedECCCgrid
year_beg=1998
year_end=2017
method=HMGFSC24

python3 make_ECCC_AR_objects.py \
    --archive-root           $data_dir \
    --input-clim-dir         $clim_data_dir \
    --input-clim-file-prefix "ERAInterim-clim-daily_" \
    --year-rng $year_beg $year_end \
    --method $method \
    --nproc 3 


