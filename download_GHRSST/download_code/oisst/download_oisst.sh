#!/bin/bash

if [ "$1" = "" ] ; then
    echo "Error: output folder needs to be provided as the first argument."
    exit 1;
fi

output_root="$1"
output_dir=$output_root/oisst

echo "Output root :: $output_root"
echo "Output dir :: $output_dir"

mkdir -p $output_dir

#for datatype in anom mean ; do
for datatype in mean ; do
for y in $( seq 2024 2025 ) ; do

    year_str=$( printf "%04d" $y )
    filename=sst.day.${datatype}.${year_str}.nc
    full_filename=$output_dir/$filename
    file_url=https://downloads.psl.noaa.gov/Datasets/noaa.oisst.v2.highres/${filename}

    if [ -f "$full_filename" ] ; then
        echo "File $full_filename already exists. Skip."
    else
        echo "File $full_filename does not exist. Download."
        echo "Download SST of year $year_str"
        wget -O $full_filename $file_url
    fi
    
done
done
