from multiprocessing import Pool
import multiprocessing
from pathlib import Path
import ARdetection_Tien
import numpy as np
import xarray as xr
import pandas as pd
import argparse

import ECCC_tools
import traceback
import os

import time

r_E = 6371e3
Nobj_max = 50
model_versions = ['GEPS5', "GEPS6"]


"""
    Assume the input file has dimension [latitude, longitude]
"""
def detectAR(ds_full, ds_clim, AR_algo):

    old_lon = ds_full.coords["longitude"].to_numpy()

    # find the lon = args.leftmost_lon
    lon_first_zero = np.argmax(ds_full.coords["longitude"].to_numpy() >= args.leftmost_lon)

    ds_full = ds_full.roll(longitude=-lon_first_zero, roll_coords=True)
    ds_clim = ds_clim.roll(longitude=-lon_first_zero, roll_coords=True)

    lat = ds_full.coords["latitude"].to_numpy() 
    lon = old_lon  % 360

    # For some reason we need to reassign it otherwise the contourf will be broken... why??? 
    ds_full = ds_full.assign_coords(longitude=lon) 
    ds_clim = ds_clim.assign_coords(longitude=lon) 
    
    IVT_x = ds_full.IVT_x[:, :].to_numpy()
    IVT_y = ds_full.IVT_y[:, :].to_numpy()
    IVT = np.sqrt(IVT_x**2 + IVT_y**2)
    IVT_clim = ds_clim.IVT[:, :].to_numpy()

    # This is in degrees
    llat, llon = np.meshgrid(lat, lon, indexing='ij')

    # dlat, dlon are in radians
    dlat = np.zeros_like(lat)

    _tmp = np.deg2rad(lat[1:] - lat[:-1])
    dlat[1:-1] = (_tmp[:-1] + _tmp[1:]) / 2.0
    dlat[0] = dlat[1]
    dlat[-1] = dlat[-2]

    if np.any(dlat <= 0):
        raise Exception("Negative dlat! %s" % (str(dlat)))

    # assume equally spaced lon
    dlon = np.deg2rad(lon[1] - lon[0])

    area = r_E**2 * np.cos(np.deg2rad(llat)) * dlon * dlat[:, None]

    if AR_algo in ["HMGFSC24", "ANOMLEN3"]: 

        # Remember, the IVT_clim here should refer to 85th percentile
        # user should provide the correct information with
        # `--AR-clim-dir` parameter
        IVT_clim[IVT_clim < 100.0] = 100.0

        labeled_array, AR_objs = ARdetection_Tien.detectARObjects(
            IVT_x, IVT_y, llat, llon, area,
            IVT_threshold=IVT_clim, # Remember, the IVT_clim here should refer to 85th percentile
            weight=IVT,
            filter_func = ARdetection_Tien.ARFilter_HMGFSC24,
        )

    else:
        raise Exception("Unknown AR algorithm: %s" % (args.AR_algo,))
      
    # Convert AR object array into Dataset format
    Nobj = len(AR_objs)
    if Nobj > Nobj_max:
        raise Exception("%d AR objs detected, exceeding the limit of %d" % (Nobj, Nobj_max))

    data_output = dict(
        feature_n    = np.zeros((Nobj_max,), dtype=int) - 1,
        area         = np.zeros((Nobj_max,),) + np.nan,
        length       = np.zeros((Nobj_max,),) + np.nan,
        centroid_lat = np.zeros((Nobj_max,),) + np.nan,
        centroid_lon = np.zeros((Nobj_max,),) + np.nan,
    )

    for i, AR_obj in enumerate(AR_objs):
        data_output['feature_n'][i] = AR_obj['feature_n']
        data_output['area'][i]      = AR_obj['area']
        data_output['length'][i]    = AR_obj['length']
        data_output['centroid_lat'][i] = AR_obj['centroid'][0]
        data_output['centroid_lon'][i] = AR_obj['centroid'][1]
    

 
    # Make Dataset
    ds_out = xr.Dataset(

        data_vars=dict(
            map          = (["latitude", "longitude"], labeled_array),
            feature_n    = (["ARobj", ], data_output["feature_n"]),
            length       = (["ARobj", ], data_output["length"]),
            area         = (["ARobj", ], data_output["area"]),
            centroid_lat = (["ARobj", ], data_output["centroid_lat"]),
            centroid_lon = (["ARobj", ], data_output["centroid_lon"]),
        ),

        coords=dict(
            longitude=(["longitude"], old_lon, dict(units="degrees_east")),
            latitude=(["latitude"], lat, dict(units="degrees_north")),
        ),

        attrs=dict(description="AR objects file."),
    )

    ds_out = ds_out.roll(longitude=lon_first_zero, roll_coords=False)
    
    return ds_out





parser = argparse.ArgumentParser(
                    prog = 'make_ECCC_AR_objects.py',
                    description = 'Postprocess ECCO data (Mixed-Layer integrated).',
)

parser.add_argument('--year-rng', type=int, nargs=2, required=True)
parser.add_argument('--archive-root', type=str, required=True)
parser.add_argument('--input-clim-dir', required=True, type=str)
parser.add_argument('--input-clim-file-prefix', type=str, required=True)
parser.add_argument('--method', required=True, type=str, choices=["HMGFSC24"])
parser.add_argument('--leftmost-lon', type=float, help='The leftmost longitude on the map. It matters when doing object detection finding connectedness.', default=0)
parser.add_argument('--nproc', type=int, default=1)
args = parser.parse_args()
print(args)

archive_root = args.archive_root
input_clim_file_prefix = args.input_clim_file_prefix

# inclusive
year_rng = args.year_rng


def doJob(job_detail, detect_phase=False):

    # phase \in ['detect', 'work']
    result = dict(job_detail=job_detail, status="UNKNOWN", need_work=False, detect_phase=detect_phase, output_file_fullpath=None)

    output_varset = "ARObjets"
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


        # Load file

        input_varset = "AR"
        input_dir = os.path.join(
            archive_root,
            "postprocessed",
            job_detail['model_version'],    
            input_varset,
        )

        input_file = "ECCC-S2S_{model_version:s}_{varset:s}_{start_time:s}.nc".format(
            model_version = job_detail['model_version'],
            varset        = input_varset,
            start_time    = job_detail['start_time'].strftime("%Y_%m-%d"),
        )

        input_file_fullpath = os.path.join(
            input_dir,
            input_file,
        )
 
        ds_in = xr.open_dataset(input_file_fullpath)

        # load clim file according to start_time + lead_time
        number_of_lead_time = ds_in.dims["lead_time"]
        number_of_members   = ds_in.dims["number"]
 
        # Make output dir
        Path(os.path.dirname(output_file)).mkdir(parents=True, exist_ok=True)
     
        ds_ARobj = [] 
        for l in range(number_of_lead_time):
            for m in range(number_of_members):

                clim_dt = job_detail['start_time'] + pd.Timedelta(days=1) * l
                
                AR_clim_file = os.path.join(
                    args.input_clim_dir,
                    "{file_prefix:s}{time_str:s}.nc".format(
                        file_prefix = input_clim_file_prefix,
                        time_str = clim_dt.strftime("%m-%d_%H"),
                    )
                )

                number_da = xr.DataArray(
                    data=[m,],
                    dims=["number"],
                )
                
                lead_time_da = xr.DataArray(
                    data=[24*(l+1),],
                    dims=["lead_time"],
                    attrs=dict(units="hours")
                )

                # Drop time dimension so that substraction can be brocasted
                ds_full = ds_in.isel(start_time=0, lead_time=l, number=m)
                ds_clim = xr.open_dataset(AR_clim_file).isel(time=0)

                _tmp = detectAR(ds_full, ds_clim, args.method)
                _tmp = _tmp.expand_dims(dim={"number": number_da}, axis=0).assign_coords(dict(number=number_da))
                _tmp = _tmp.expand_dims(dim={"lead_time": lead_time_da}, axis=0).assign_coords(dict(lead_time=lead_time_da))
                ds_ARobj.append(_tmp)

        ds_ARobj = xr.merge(ds_ARobj)

        start_time_da = xr.DataArray(
            data=[ int( (job_detail['start_time'] - pd.Timestamp("1970-01-01") ) / pd.Timedelta(hours=1)),],
            dims=["start_time"],
            attrs=dict(units="hours since 1970-01-01 00:00:00")
        )

        ds_ARobj = ds_ARobj.expand_dims(dim={"start_time": start_time_da}, axis=0).assign_coords(dict(start_time=start_time_da))


        print(ds_ARobj)

        print("Writing to file: %s" % (output_file_fullpath,) )
        ds_ARobj.to_netcdf(
            output_file_fullpath,
            unlimited_dims=["time",],
        )

        result['status'] = 'OK'

    except Exception as e:

        print("[%s] Error. Now print stacktrace..." % (start_time_str,))
        import traceback
        traceback.print_exc()


    return result



failed_dates = []
dts_in_year = pd.date_range("2021-01-01", "2021-12-31", inclusive="both")
#dts_in_year = pd.date_range("2021-01-03", "2021-01-03", inclusive="both")
input_args = []
for model_version in model_versions:
    
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
                
               


with Pool(processes=args.nproc) as pool:

    results = pool.starmap(doJob, input_args)

    for i, result in enumerate(results):
        if result['status'] != 'OK':
            #print('!!! Failed to generate output file %s.' % (",".join(result['output_file_fullpath'],)))
            print('!!! Failed to generate output file %s.' % (result['output_file_fullpath'],))
            failed_dates.append(result['job_detail'])


print("Tasks finished.")

print("Failed output files: ")
for i, failed_detail in enumerate(failed_dates):
    print("%d : " % (i+1), failed_detail)

print("Done.")

