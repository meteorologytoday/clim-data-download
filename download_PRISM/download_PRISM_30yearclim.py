with open("shared_header.py", "rb") as source_file:
    code = compile(source_file.read(), "shared_header.py", "exec")
exec(code)


import traceback
import numpy as np
import pandas as pd
import xarray as xr
import shutil
from pathlib import Path
import urllib.request
import rasterio
from rasterio.transform import xy

dataset_name = "PRISM-30yr"
prefix = "PRISM_30yr"

output_dir = Path("data") / dataset_name
tmp_dir    = Path("data") / dataset_name / "tmp"

print("Create dir: %s" % (output_dir,))
output_dir.mkdir(parents=True, exist_ok=True)

if tmp_dir.exists():
    print("Remove temporary directory: ", tmp_dir)
    shutil.rmtree(tmp_dir)

print("Create dir: %s" % (tmp_dir,))
tmp_dir.mkdir(parents=True, exist_ok=True)



def ifSkip(dt):

    skip = False
    if not ( dt.month in [1,] ):
        skip = True

    return skip

nproc = 1

beg_time = "2021-01-01"
end_time = "2021-12-31"
dts = pd.date_range(beg_time, end_time, freq="D", inclusive="both")

varnames = [
    "ppt",
]


def work(details):

    result = dict(
        details = details,
        status = "UNKNOWN",
        need_work = True,
    )

    dt = details["dt"]
    output_dir = Path(details["output_dir"])
    varname = details["varname"]
    detect_phase = details["detect_phase"]
    
    try:
                    
        full_time_str = dt.strftime("%m-%d") 
        
        tmp_filename = "{prefix:s}-{varname:s}-{full_time_str:s}.zip".format(
            prefix = "PRISM",
            varname = varname,
            full_time_str = full_time_str,
        )

        output_filename = "{prefix:s}-{varname:s}-{full_time_str:s}.nc".format(
            prefix = "PRISM",
            varname = varname,
            full_time_str = full_time_str,
        )


        fixed_prefix = "PRISM_{varname:s}_30yr_normal_4kmD1_{md:s}_bil".format(
            varname = varname,
            md = dt.strftime("%m%d"),
        )

        original_filename = "%s.zip" % fixed_prefix
        bil_filename = "%s.bil" % fixed_prefix

        url = "https://ftp.prism.oregonstate.edu/normals/daily/4km/{varname:s}/{filename:s}".format(
            varname = varname,
            filename = original_filename,
        )
       
        full_output_file = output_dir / output_filename
        full_tmp_file = tmp_dir / tmp_filename

        if detect_phase:

            if full_output_file.exists():

                result["need_work"] = False
                #print("File %s already exists." % (full_output_file,))
                
            result["status"] = "OK"
            return result             

        print("Going to download %s => %s" % (url, full_output_file,))

        # Download the file from `url` and save it locally under `file_name`:
        with urllib.request.urlopen(url) as response, open(full_tmp_file, 'wb') as out_file:
            shutil.copyfileobj(response, out_file)
        
        
        filepath = "zip+file://{external_file:s}!{internal_file:s}".format(
             external_file = str(full_tmp_file),
            internal_file = "/%s" % bil_filename,
        )

        print("filepath: ", filepath)
        
        with rasterio.open(filepath) as ds_tmp: 
            
            data = ds_tmp.read().copy()
            
            data[data == -9999] = np.nan
            
            tsfm = ds_tmp.transform  # Affine transformation

            # Get the number of rows and columns
            nrows, ncols = ds_tmp.height, ds_tmp.width

            # Generate 1D arrays of x (longitude) and y (latitude) coordinates
            # Get the x-coordinates (longitudes) for all columns of the first row
            # Get the y-coordinates (latitudes) for all rows of the first column
            lon = np.array([xy(tsfm, 0, col)[0] for col in range(ncols)]) % 360.0
            lat = np.array([xy(tsfm, row, 0)[1] for row in range(nrows)])

            ds_output = xr.Dataset(
                data_vars = dict(
                    total_precipitation = (["time", "lat", "lon"], data),
                ), coords = dict(
                    time = ("time", [dt,]),
                    lat = (["lat",], lat),
                    lon = (["lon",], lon),
                ),
            )

            print("Writing output: ", full_output_file)
            ds_output.to_netcdf(full_output_file, unlimited_dims="time")


        for remove_file in [full_tmp_file,]:
            if remove_file.exists():
                print("[%s] Remove file: `%s` " % (dt, remove_file))
                os.remove(remove_file)

 
        result["status"] = "OK"
                
    except Exception as e:

        traceback.print_exc()
        result["status"] = "ERROR"
    

    return result




failed_dates = []


input_args = []

for dt in dts:

    y = dt.year
    m = dt.month
    time_str = dt.strftime("%m-%d")

    if ifSkip(dt):
        print("Skip the date: %s" % (time_str,))
        continue

    for varname in varnames:
   
        result = work(dict(dt=dt, output_dir=output_dir, varname=varname, detect_phase=True))
        
        if result['status'] != 'OK':
            print("[detect] Failed to detect variable `%s` of date %s " % (varname, str(dt)))
        
        if result['need_work'] is False:
            print("[detect] File all exist for (date, varname) =  (%s, %s)." % (time_str, varname))
        else:
            input_args.append((dict(dt=dt, output_dir=output_dir, varname=varname, detect_phase=False),))
        
#print("Create dir: %s" % (download_tmp_dir,))
#Path(download_tmp_dir).mkdir(parents=True, exist_ok=True)

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
