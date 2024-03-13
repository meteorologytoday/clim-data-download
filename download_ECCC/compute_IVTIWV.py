import xarray as xr
import numpy as np
import pandas as pd
import ECCC_tools
import traceback
from multiprocessing import Pool
import multiprocessing
from pathlib import Path
import os

archive_root = "ECCC_data/data20"

nproc = 1

# inclusive
year_rng = [1998, 2017]

model_versions = ['GEPS5', "GEPS6"]

g0 = 9.81

def computeARvariables(ds):
   
    ds = ds.where((ds.coords["level"] <= 1000) & (ds.coords["level"] >=200), drop=True)
 
    lev = ds.coords["level"].to_numpy()
    print(lev)
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

    print(lev_weight)

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


        start_time = job_detail['start_time']

        start_time_str = job_detail['start_time'].strftime("%Y-%m-%d")
       
        print("[%s] Start job." % (start_time_str,))
 
        output_dir = os.path.join(
            archive_root,
            "postprocessed",
            job_detail['model_version'],    
            output_varset,
        )

        output_file = "ECCC-S2S_{model_version:s}_{varset:s}_{start_time:s}.nc".format(
            model_version = job_detail['model_version'],
            varset        = output_varset,
            start_time    = job_detail['start_time'].strftime("%Y_%m-%d"),
        )

        output_file_fullpath = os.path.join(
            output_dir,
            output_file,
        )
        
        result['output_file_fullpath'] = output_file_fullpath
 
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # First round is just to decide which files
        # to be processed to enhance parallel job 
        # distribution. I use variable `phase` to label
        # this stage.
        file_exists = os.path.isfile(output_file_fullpath)

        if detect_phase is True:
            result['need_work'] = not file_exists
            result['status'] = 'OK' 
            return result

        if file_exists:
            print("[%s] Output file `%s` already exists. Skip." % (start_time_str, output_file_fullpath))
        else:
            # First, load UVTZ and Q


            ds_UVTZ = ECCC_tools.open_dataset("raw", "UVTZ", job_detail['model_version'], job_detail['start_time'])
            ds_Q    = ECCC_tools.open_dataset("raw", "Q",    job_detail['model_version'], job_detail['start_time'])

            #print(ds_Q.coords["level"])
            ds_UVTZ = ds_UVTZ.sel(level=ds_Q.coords["level"])
            
            ds_QUVTZ = xr.merge([ds_Q, ds_UVTZ]) 

            ds_AR = computeARvariables(ds_QUVTZ)
       
            print("[%s] Writing output file: %s" % (start_time_str, output_file_fullpath,)) 
            ds_AR.to_netcdf(
                output_file_fullpath, unlimited_dims="time",
                encoding={'start_time':{'units':'hours since 1970-01-01 00:00:00'}}
            )
            if os.path.isfile(output_file_fullpath):
                print("[%s] File `%s` is generated." % (start_time_str, output_file_fullpath,))
            
        result['status'] = 'OK'

    except Exception as e:

        result['status'] = 'ERROR'
        traceback.print_stack()
        traceback.print_exc()
        print(e)

    print("[%s] Done. " % (start_time_str,))

    return result



failed_dates = []
dts_in_year = pd.date_range("2021-01-31", "2021-12-31", inclusive="both")
input_args = []
for model_version in ["GEPS5", "GEPS6"]:
    
    print("[MODEL VERSION]: ", model_version)
    
    for dt in dts_in_year:
        
        model_version_date = ECCC_tools.modelVersionReforecastDateToModelVersionDate(model_version, dt)
        
        if model_version_date is None:
            continue
        
        print("The date %s exists on ECMWF database. " % (dt.strftime("%m/%d")))

        for year in range(year_rng[0], year_rng[1]+1):
            
            month = dt.month
            day = dt.day
            start_time = pd.Timestamp(year=year, month=month, day=day)

            print("[Detect] Checking date %s" % (start_time.strftime("%Y-%m-%d"),))

            job_detail = dict(
                model_version = model_version,
                start_time = start_time, 
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
            print('!!! Failed to generate output file %s.' % (",".join(result['output_file'],)))
            failed_dates.append(result['job_detail'])


print("Tasks finished.")

print("Failed output files: ")
for i, failed_output_file in enumerate(failed_output_files):
    print("%d : %s" % (i+1, failed_output_file,))

print("Done.")

