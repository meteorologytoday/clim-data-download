#!/bin/bash

if [ "$1" = "" ] ; then
    echo "Error: output folder needs to be provided as the first argument."
    exit 1;
fi

if [ "$2" = "" ] ; then
    echo "Error: Start date needs to be provided as the second argument."
    exit 1;
fi

if [ "$3" = "" ] ; then
    echo "Error: End date needs to be provided as the third argument."
    exit 1;
fi

output_root="$1"
echo "Output root :: $output_root"

start_date="$2"
end_date="$3"

echo "Start date :: $start_date"
echo "End   date :: $end_date"

spatial_selector="-180,0,180,90"


dataset_details=(
    OSTIA_UKMO     OSTIA-UKMO-L4-GLOB-v2.0
#    MUR_JPL        MUR-JPL-L4-GLOB-v4.1
#    GPBN_OSPO      Geo_Polar_Blended_Night-OSPO-L4-GLOB-v1.0
#    K10SST_NAVO    K10_SST-NAVO-L4-GLOB-v01
#    GAMSSA_ABOM    GAMSSA_28km-ABOM-L4-GLOB-v01
#    DMIOI_DMI      DMI_OI-DMI-L4-GLOB-v1.0
)

nparams=2
N=$(( ${#dataset_details[@]} / $nparams ))
echo "We have $N entries..."
for i in $( seq 1 $N ) ; do

    dataset="${dataset_details[$(( (i-1) * $nparams + 0 ))]}"
    dataset_label="${dataset_details[$(( (i-1) * $nparams + 1 ))]}"

    echo "Downloading dataset: $dataset => $dataset_label"

    output_dir=$output_root/$dataset
    echo "Output dir :: $output_dir"
    mkdir -p $output_dir

    podaac-data-downloader          \
        -c $dataset_label           \
        -d $output_dir              \
        --start-date $start_date    \
        --end-date $end_date        \
        -b="$spatial_selector"

done
