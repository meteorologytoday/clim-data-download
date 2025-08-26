#!/bin/bash


python3 download_TRMM.py \
    --date-range 2015-09-25 2016-03-05 \
    --archive-root /data/SO3/t2hsu/data/TRMM \
    --hours-skip 3 \
    --nproc 5
