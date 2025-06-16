#!/bin/bash

archive_root=/data/SO3/t2hsu/data/ECMWF_S2S
date_beg=2024-12-01
date_end=2025-02-01

#date_beg=2024-06-01
#date_end=2024-08-01


echo "Running download_S2S.py"
python3 download_S2S.py                \
    --origin rjtd                      \
    --nwp-type forecast                \
    --varsets surf_avg                 \
    --archive-root $archive_root       \
    --date-range $date_beg $date_end   \
    --model-versions CPS3              \
    --numbers 0-4 \
    --nproc 3


if [ ] ; then
echo "Running download_S2S.py"
python3 download_S2S.py                \
    --origin kwbc                      \
    --nwp-type forecast                \
    --varsets surf_avg                 \
    --archive-root $archive_root       \
    --date-range $date_beg $date_end   \
    --model-versions CFSv2             \
    --numbers 0-15 \
    --nproc 3
#fi

#if [ ] ; then
echo "Running download_S2S.py"
python3 download_S2S.py                \
    --origin ecmf                      \
    --nwp-type forecast                \
    --varsets surf_avg                 \
    --archive-root $archive_root       \
    --date-range $date_beg $date_end   \
    --model-versions CY48R1         \
    --numbers 0-20 \
    --nproc 3
#fi

echo "Running download_S2S.py"
python3 download_S2S.py                \
    --origin cwao                      \
    --nwp-type forecast                \
    --varsets surf_avg                 \
    --archive-root $archive_root       \
    --date-range $date_beg $date_end   \
    --model-versions GEPS8             \
    --numbers 0-20 \
    --nproc 3
fi
