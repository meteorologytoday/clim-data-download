#!/bin/bash

work_root=`pwd`

export PYTHONPATH=$work_root/lib:$PYTHONPATH

echo "PYTHONPATH = $PYTHONPATH"

data_root=/data/SO3/t2hsu/data/SST/
rawdata_root=$data_root/raw
ppdata_root=$data_root/postprocessed




