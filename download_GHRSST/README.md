# project-SST-spectrum



## Download data

In the folder `download_data`. In general run `download.sh` and then `python3 postprocess.py`.

- Script `100_download_and_postprocess_data_oisst.sh` does OISST.
- Script `101_download_and_postprocess_data_GHRSST.sh` does GHRSST.


## Regridding

- Run `121_crop_SST.sh`.
- After regridding, run `122_generate_GHRSST_mean.sh` to generate the mean of multiple product.
