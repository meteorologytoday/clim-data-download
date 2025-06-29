#!/bin/bash
source 98_trapkill.sh

nproc=5

res_deg=0.1
label="NPAC_${res_deg}"

input_datasets=(
    MUR_JPL
    OSTIA_UKMO
    DMIOI_DMI
    GAMSSA_ABOM
    K10SST_NAVO
    GPBN_OSPO
)

output_dataset=GHRSST


for y in $( seq 2018 2022 ) ; do 

    y2=$(( $y + 1 ))

    python3 src/gen_mean_dataset.py             \
        --input-datasets "${input_datasets[@]}" \
        --output-dataset $output_dataset        \
        --datatype cropped                      \
        --label   $label                        \
        --varname sst                           \
        --timepentad-rng ${y}P66 ${y2}P12       \
        --nproc $nproc

    echo "Done."


done
