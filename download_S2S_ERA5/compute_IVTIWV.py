import xarray as xr
import numpy as np
import pandas as pd
import ERA5_tools
import traceback
from multiprocessing import Pool
import multiprocessing
from pathlib import Path
import os

date_rng = [
    "1998-01-01",
    "2018-02-02",
]

archive_root = "S2S/ERA5"
nproc = 10

# in hPa
selected_levs = [
    200, 300, 500, 700, 850, 925, 1000,
]


print("Date begin: ", date_rng[0])
print("Date end  : ", date_rng[1])
print("Selected levs : ", selected_levs)
print("archive_root = ", archive_root)
print("nproc = ", nproc)

g0 = 9.81

def computeARvariables(ds):
   
    ds = ds.where((ds.coords["level"] <= 1000) & (ds.coords["level"] >=200), drop=True)
 
    lev = ds.coords["level"].to_numpy()
    
    #print(lev)
    _lev_weight = np.zeros((len(lev),))
    dlev = lev[1:] - lev[:-1]
    _lev_weight[1:-1] = (dlev[1:] + dlev[:-1]) / 2
    _lev_weight[0]    = dlev[0]  / 2
    _lev_weight[-1]   = dlev[-1] / 2
    _lev_weight *= 100.0 / g0  # convert into density
    

    if np.any(_lev_weight <= 0):
        raise Exception("Bad lev_weight: %s " % (str(_lev_weight),))

    lev_weight = xr.DataArray(
        data=_lev_weight,
        dims=["level",],
        coords=dict(
            level=ds.coords["level"],
        ),
    )

    #print(lev_weight)

    IWV   = ds["q"].weighted(lev_weight).sum(dim="level")
    IVT_x = (ds["q"] * ds["u"]).weighted(lev_weight).sum(dim="level")
    IVT_y = (ds["q"] * ds["v"]).weighted(lev_weight).sum(dim="level")
    IVT   = (IVT_x**2 + IVT_y**2)**0.5
    IWVKE = (ds["q"] * (ds["u"]**2.0 + ds["v"]**2.0)).weighted(lev_weight).sum(dim="level")
    
    IWV   = IWV.rename("IWV")
    IVT_x = IVT_x.rename("IVT_x")
    IVT_y = IVT_y.rename("IVT_y")
    IVT   = IVT.rename("IVT")
    IWVKE = IWVKE.rename("IWVKE")

    ds_AR = xr.merge([IWV, IVT_x, IVT_y, IVT, IWVKE])

    return ds_AR 
    


def doJob(job_detail, detect_phase=False):

    # phase \in ['detect', 'work']
    result = dict(job_detail=job_detail, status="UNKNOWN", need_work=False, detect_phase=detect_phase, output_file_fullpath=None)

    output_varset = "AR"
    try:


        dt = job_detail['dt']
        dt_str = dt.strftime("%Y-%m-%d")
       
        print("[%s] Start job." % (dt_str,))


        output_file_fullpath = ERA5_tools.generate_filename(output_varset, dt, "inst")
        output_dir = os.path.dirname(output_file_fullpath)
        output_file = os.path.basename(output_file_fullpath)

        result['output_file_fullpath'] = output_file_fullpath

        # First round is just to decide which files
        # to be processed to enhance parallel job 
        # distribution. I use variable `phase` to label
        # this stage.
        file_exists = os.path.isfile(output_file_fullpath)

        if detect_phase is True:
            result['need_work'] = not file_exists
            result['status'] = 'OK' 
            return result

        # This if-statment is just a double check
        if file_exists:
            print("[%s] Output file `%s` already exists. Skip." % (start_time_str, output_file_fullpath))
        else:
                
            Path(output_dir).mkdir(parents=True, exist_ok=True)

            # First, load UVTZ and Q
            ds_u = ERA5_tools.open_dataset("u_component_of_wind", dt, "inst")
            ds_v = ERA5_tools.open_dataset("v_component_of_wind", dt, "inst")
            ds_q = ERA5_tools.open_dataset("specific_humidity", dt, "inst")

            
            ds_u = ds_u.sel(level=selected_levs)
            ds_v = ds_v.sel(level=selected_levs)
            ds_q = ds_q.sel(level=selected_levs)

            ds_uvq = xr.merge([ds_u, ds_v, ds_q]) 

            ds_AR = computeARvariables(ds_uvq)
       
            print("[%s] Writing output file: %s" % (dt_str, output_file_fullpath,)) 
            ds_AR.to_netcdf(
                output_file_fullpath, unlimited_dims="time",
                encoding={'time':{'units':'hours since 1970-01-01 00:00:00'}}
            )

            if os.path.isfile(output_file_fullpath):
                print("[%s] File `%s` is generated." % (dt, output_file_fullpath,))
            else:
                print("[%s] Failed to generate file `%s`" % (dt_str, output_file_fullpath,))            
        result['status'] = 'OK'

    except Exception as e:

        result['status'] = 'ERROR'
        traceback.print_exc()
        print(e)

    print("[%s] Done. " % (dt_str,))

    return result



failed_dates = []
dts = pd.date_range(date_rng[0], date_rng[1], inclusive="both")
input_args = []
    
for dt in dts:
    
    print("[Detect] Checking date %s" % (dt.strftime("%Y-%m-%d"),))

    job_detail = dict(
        dt = dt,
    )

    result = doJob(job_detail, detect_phase=True)

    if not result['need_work']:
        print("File `%s` already exist. Skip it." % (result['output_file_fullpath'],))
        continue
    
    input_args.append((job_detail, False))
        
       


with Pool(processes=nproc) as pool:

    results = pool.starmap(doJob, input_args)

    for i, result in enumerate(results):
        if result['status'] != 'OK':
            print('!!! Failed to generate output file %s.' % (result['output_file_fullpath'],))
            failed_dates.append(result['job_detail'])


print("Tasks finished.")

print("Failed output files: ")
for i, failed_detail in enumerate(failed_dates):
    print("%d : %s" % (i+1, str(failed_detail['dt']),))

print("Done.")

