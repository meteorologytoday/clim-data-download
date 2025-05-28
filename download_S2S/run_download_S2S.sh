#!/bin/bash

archive_root=/data/SO3/t2hsu/data/ECMWF_S2S

python3 download_S2S.py                \
    --origin ecwf                      \
    --nwp-type forecast                \
    --varsets surf_avg                 \
    --archive-root $archive_root       \
    --date-range 2024-12-01 2024-12-05 \
    --model-versions "CY48R1"           \
    --nproc 1

