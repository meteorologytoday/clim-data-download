#!/bin/bash

archive_root=./ECCC_data/data_test

if [ ] ; then
python3 download_ECCC_new.py \
    --nwp-type hindcast \
    --archive-root $archive_root \
    --date-range 2015-01-01 2015-01-05 \
    --model-versions "GEPS6"
fi

python3 download_ECCC_new.py           \
    --nwp-type forecast                \
    --varsets surf_avg surf_inst       \
    --archive-root $archive_root       \
    --date-range 2024-10-01 2025-04-01 \
    --model-versions "GEPS8"           \
    --nproc 2




