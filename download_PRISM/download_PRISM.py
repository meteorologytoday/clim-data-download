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


dataset_name = "PRISM"
prefix = "PRISM"

output_dir = Path("data") / dataset_name
print("Create dir: %s" % (output_dir,))
output_dir.mkdir(parents=True, exist_ok=True)


def ifSkip(dt):

    skip = False
#    if not ( dt.month in [10, 11, 12, 1, 2, 3, 4] ):
#        skip = True

    return skip

nproc = 5

dts = pd.date_range(beg_time.strftime("%Y-%m-%d"), end_time.strftime("%Y-%m-%d"), freq="D", inclusive="both")

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
                    
        full_time_str = dt.strftime("%Y-%m-%d") 
        
        output_filename = "{prefix:s}-{varname:s}-{full_time_str:s}.zip".format(
            prefix = "PRISM",
            varname = varname,
            full_time_str = full_time_str,
        )

        original_filename = "PRISM_ppt_stable_4kmD1_{ymd:s}_bil.zip".format(
            ymd = dt.strftime("%Y%m%d"),
        )

        url = "https://ftp.prism.oregonstate.edu/data_archive/{varname:s}/daily_D1_201506/{y:s}/{filename:s}".format(
            y = dt.strftime("%Y"),
            ymd = dt.strftime("%Y%m%d"),
            varname = varname,
            filename = original_filename,
        )
       
        full_output_file = output_dir / output_filename

                

        
            
        if detect_phase:

            if full_output_file.exists():

                result["need_work"] = False
                #print("File %s already exists." % (full_output_file,))
                
            result["status"] = "OK"
            return result             

        print("Going to download %s => %s" % (url, full_output_file,))

        # Download the file from `url` and save it locally under `file_name`:
        with urllib.request.urlopen(url) as response, open(full_output_file, 'wb') as out_file:
            shutil.copyfileobj(response, out_file)
        
                 
                   
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
    time_str = dt.strftime("%Y-%m-%d")

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
            print('!!! Failed to generate output of date %s.' % (result['details']['dt'].strftime("%Y-%m-%d"), ))

            failed_dates.append(result['details']['dt'])


print("Tasks finished.")

print("Failed dates: ")
for i, failed_date in enumerate(failed_dates):
    print("%d : %s" % (i+1, failed_date.strftime("%Y-%m"),))


print("Done.")
