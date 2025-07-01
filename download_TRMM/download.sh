#!/bin/bash


python3 download_TRMM.py \
    --date-range 2017-12-01 2017-12-02 \
    --archive-root /data/SO3/t2hsu/data/TRMM \
    --hours-skip 3 \
    --nproc 1
