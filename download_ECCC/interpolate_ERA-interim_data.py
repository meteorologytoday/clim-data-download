from multiprocessing import Pool
import xarray as xr
import numpy as np
import argparse
import pandas as pd
import os.path
import os
from pathlib import Path


parser = argparse.ArgumentParser(
                    prog = 'make_climatology.py',
                    description = 'Use ncra to do daily climatology',
)
parser.add_argument('--input-dir', required=True, help="Input directory.", type=str)
parser.add_argument('--output-dir', required=True, help="Output directory.", type=str)
parser.add_argument('--filename-prefix', required=True, help="The prefix of filename.", type=str)
parser.add_argument('--output-latlon', required=True, help="An netcdf file containing desired lat-lon.", type=str) 
parser.add_argument('--nproc', type=int, default=1)
args = parser.parse_args()
print(args)



print("Loading output lat lon file: ", args.output_latlon)

ds_latlon = xr.open_dataset(args.output_latlon)

print(ds_latlon)


out_lat = ds_latlon.coords["latitude"]
out_lon = ds_latlon.coords["longitude"]



def work(dt, detect_phase=False):

    result = dict(status="UNKNOWN", dt=dt, output_filename="", detect_phase=detect_phase)

    try:

            
        mmddhh_str = dt.strftime("%m-%d_%H")
        
        output_filename = os.path.join(
            args.output_dir,
            "{output_filename_prefix:s}{mmddhh:s}.nc".format(
                output_filename_prefix = args.filename_prefix,
                mmddhh = mmddhh_str,
            )
        )
 
        input_filename = os.path.join(
            args.input_dir,
            "{input_filename_prefix:s}{mmddhh:s}.nc".format(
                input_filename_prefix = args.filename_prefix,
                mmddhh = mmddhh_str,
            )
        )
        
        result["output_filename"] = output_filename
        result["input_filename"]  = input_filename

        if detect_phase:
            
            result["need_work"] = not os.path.exists(output_filename)
            result["status"] = "OK"
            
            return result
 

        else:

            # Create output directory if not exists
            dir_name = os.path.dirname(output_filename) 
            Path(dir_name).mkdir(parents=True, exist_ok=True)
            
            ds_in = xr.open_dataset(input_filename)
            print(ds_in)

            # interpolation magic
            ds_out = ds_in.interp(
                coords=dict(
                    latitude=out_lat,
                    longitude=out_lon,
                ),
                method='linear',
            )
            
            print("[%s] Outputting file: %s" % (mmddhh_str, output_filename,))
            ds_out.to_netcdf(
                output_filename,
                unlimited_dims="time",
            )
            
            ds_in.close()
            ds_out.close()
            
        result["status"] = "OK"

    except Exception as e:

        print("[%s] Error. Now print stacktrace..." % (mmddhh_str,))
        import traceback
        traceback.print_exc()
        
        result["status"] = "ERROR"

    return result

def ifSkip(dt):

    return False

failed_dates = []
dts = pd.date_range("2000-01-01", "2001-01-01", freq="D", inclusive="left")

input_args = []

for dt in dts:

    dt_str = dt.strftime("%m-%d")


    if ifSkip(dt):
        print("Skip : %s" % (dt_str,))
        continue

    print("Detect date: ", dt_str)
    result = work(dt, detect_phase=True)
    
    if result['status'] != 'OK':
        print("[detect] Failed to detect month %d " % (month,))
    
    else:
        if result['need_work'] is True:
            print("[detect] Need to work on %s." % (dt_str,))
            input_args.append((dt,))
        else:
            print("[detect] Files all exist for month =  %s." % (dt_str,))




print("Ready to do work...")
with Pool(processes=args.nproc) as pool:

    results = pool.starmap(work, input_args)

    for i, result in enumerate(results):
        if result['status'] != 'OK':
            print('!!! Failed to generate output of month %s.' % (result['dt'].strftime("%m-%d"),))
            failed_dates.append(result['dt'])


print("Tasks finished.")

if len(failed_dates) == 0:
    print("Congratualations! All dates are successfully produced.")    
else:
    print("Failed dates: ")
    for i, failed_date in enumerate(failed_dates):
        print("[%d] %s" % (i, failed_date.strftime("%m-%d"),) )

print("Done.")       
