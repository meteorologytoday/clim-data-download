#!/bin/bash

archive_root=/data/SO3/t2hsu/data/ECMWF_S2S

echo "Running download_S2S.py"
python3 download_S2S.py                \
    --origin ecmf                      \
    --nwp-type forecast                \
    --varsets surf_avg                 \
    --archive-root $archive_root       \
    --date-range 2024-12-01 2025-04-01 \
    --model-versions CY49R1         \
    --numbers 0-20 \
    --nproc 3

