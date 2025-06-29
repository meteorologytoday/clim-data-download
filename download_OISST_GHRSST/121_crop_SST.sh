#!/bin/bash

source 000_setup.sh
source 98_trapkill.sh

nproc=1

res_deg=0.1
label="NPAC_${res_deg}"

datasets=(
    oisst
    MUR_JPL
#    K10SST_NAVO
    OSTIA_UKMO
    DMIOI_DMI
    GAMSSA_ABOM
    GPBN_OSPO
)


#for dataset in MUR oisst ostia ; do

nproc_cnt=0
#for y in $( seq 2007 2024 ) ; do
for y in $( seq 2024 2024 ) ; do
for dataset in "${datasets[@]}"; do
    
    
    python3 src/regrid/crop_SST.py \
        --dataset $dataset \
        --label $label \
        --timepentad-rng ${y}P-5 ${y}P-3 \
        --lat-rng 0 70    \
        --lon-rng 120 300 \
        --res-deg $res_deg &

    nproc_cnt=$(( $nproc_cnt + 1 ))
    if (( $nproc_cnt >= $nproc )) ; then
        echo "Max batch_cnt reached: $nproc"
        wait
        nproc_cnt=0
    fi

done
done

wait
