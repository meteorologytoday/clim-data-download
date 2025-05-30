#!/bin/bash

source 000_setup.sh
source 98_trapkill.sh

nproc=1

res_deg=0.1
label="NPAC_${res_deg}"

datasets=(
    MUR_JPL
#    K10SST_NAVO
    OSTIA_UKMO
    DMIOI_DMI
    GAMSSA_ABOM
    GPBN_OSPO
)


nproc_cnt=0
for y in $( seq 2024 2024 ) ; do
for dataset in "${datasets[@]}"; do
    
    
    python3 src/regrid/generate_GHRSST_mean.py       \
        --input-root $data_dir    \
        --datasets ${datasets[@]} \
        --label $label            \
        --timepentad-rng ${y}P-5 ${y}P0 &

    nproc_cnt=$(( $nproc_cnt + 1 ))
    if (( $nproc_cnt >= $nproc )) ; then
        echo "Max batch_cnt reached: $nproc"
        wait
        nproc_cnt=0
    fi

done
done

wait
