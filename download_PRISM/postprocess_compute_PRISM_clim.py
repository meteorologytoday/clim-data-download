import traceback
import numpy as np
import pandas as pd
import xarray as xr
import shutil
from pathlib import Path
import urllib.request
import rasterio
from rasterio.transform import xy

from multiprocessing import Pool
import multiprocessing
import os
import PRISM_tools

from pathlib import Path

def pleaseRun(cmd):
    print(">> %s" % cmd)
    os.system(cmd)

nproc = 1

# This is for PRISM
beg_year = 1991
end_year = 2020

half_window = 5

print("Clim year range: %d ~ %d" % (PRISM_tools.year_rng_clim[0], PRISM_tools.year_rng_clim[1]))

PRISM_tools.dataset_dir_clim.mkdir(parents=True, exist_ok=True)

def ifSkip(dt):

    skip = False
    if not ( dt.month in [1,] ):
        skip = True

    return skip



def work(details):

    result = dict(
        details = details,
        status = "UNKNOWN",
        need_work = True,
    )

    dt = details["dt"]
    year_rng = details["year_rng"]
    half_window = pd.Timedelta(days=details["half_window"])
    detect_phase = details["detect_phase"]
    
    dt_str = dt.strftime("%m-%d")
    try:
        
        
        output_file = Path(PRISM_tools.getClimPRISMFilename(dt))
        
        
        if detect_phase:

            if output_file.exists():

                result["need_work"] = False
                #print("File %s already exists." % (full_output_file,))
                
            result["status"] = "OK"
            return result             

        print("Going to compute climatology to generate file: %s" % (output_file,))
        avg_filenames = []
        for year in range(year_rng[0], year_rng[1]+1):
            dt_center = pd.Timestamp(year=year, month=dt.month, day=dt.day)
            for _dt in pd.date_range(dt_center - half_window, dt_center + half_window, freq="D"):
                avg_filenames.append(PRISM_tools.getPRISMFilename(_dt))
       
        print("Loading files...")
        # It seems xarray has trouble loading PRISM files...
        # It keeps complaining unable to combine coordinate.
        data = []
        for fname in avg_filenames:
            with xr.open_dataset(fname) as ds:
                data.append(ds["total_precipitation"].isel(time=0).to_numpy())
                

        ds = xr.open_dataset(avg_filenames[0])
        lat = ds.coords["lat"]
        lon = ds.coords["lon"]

        data = np.stack(data, axis=0)

        stat_mean = np.mean(data, axis=0, keepdims=True)
        stat_std  = np.std(data, axis=0, ddof=1, keepdims=True)
        
        new_ds = xr.Dataset(
            data_vars = dict(
                total_precipitation_mean = (["time", "lat", "lon"], stat_mean),
                total_precipitation_std  = (["time", "lat", "lon"], stat_std),
            ),
            coords = dict(
                lat = (["lat", ], lat.data),
                lon = (["lon", ], lon.data),
                time = (["time", ], [dt,]),
            ),
        )

        #print("File merged.")
        #da = da["total_precipitation"]
        #da = xr.open_mfdataset(avg_filenames, data_vars=["total_precipitation"])["total_precipitation"]
    
        #print(da)
   
        #print("Compute statistics...") 
        #da_avg = da.mean(dim="time").rename("total_precipitation_mean")
        #da_std = da.std(dim="time").rename("total_precipitation_std")

        """
        print("Merging...")
        new_ds = xr.merge([da_avg, da_std])
        print("Expand...")
        new_ds = new_ds.expand_dims(dim={"time": [dt,]}, axis=0)
        """
        print(new_ds) 
        print("Writing output: ", output_file) 
        new_ds.to_netcdf(output_file, unlimited_dims="time")
        print("Done.")
        result["status"] = "OK"
                
    except Exception as e:

        traceback.print_exc()
        result["status"] = "ERROR"
    

    return result




failed_dates = []
input_args = []

dts = pd.date_range("2001-01-01", "2001-12-31", freq="D")
for dt in dts:

    m = dt.month
    d = dt.day
    time_str = dt.strftime("%m-%d")

    if ifSkip(dt):
        print("Skip date: ", time_str)
        continue

    details = dict(
        dt=dt, 
        year_rng = PRISM_tools.year_rng_clim,
        half_window = half_window,
    )

    details['detect_phase'] = True
    result = work(details)
     
    if result['status'] != 'OK':
        print("[detect] Failed to detect date %s " % (time_str,))
    
    if result['need_work'] is False:
        print("[detect] File all exist for date =  %s." % (time_str,))
    else:
        
        details['detect_phase'] = False
        input_args.append((details,))
    
with Pool(processes=nproc) as pool:

    results = pool.starmap(work, input_args)

    for i, result in enumerate(results):
        if result['status'] != 'OK':
            print('!!! Failed to generate output of date %s.' % (result['details']['dt'].strftime("%m-%d"), ))

            failed_dates.append(result['details']['dt'])


print("Tasks finished.")

print("Failed dates: ")
for i, failed_date in enumerate(failed_dates):
    print("%d : %s" % (i+1, failed_date.strftime("%m-%d"),))


print("Done.")
