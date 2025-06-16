print("Loading library")
import numpy as np
import xarray as xr
import traceback
import os
from pathlib import Path 
import pandas as pd
import argparse
import data_loader
print("Done")

def findfirst(a):
    
    for i in range(len(a)):
        if a[i]:
            return i

    return -1


dataset_details=dict(

    K10SST_NAVO = dict(
        suffix = "NAVO-L4_GHRSST-SST1m-K10_SST-GLOB-v02.0-fv01.0",
        timestr = "000000",
    ),
   
    
    OSTIA_UKMO = dict(
        suffix = "UKMO-L4_GHRSST-SSTfnd-OSTIA-GLOB-v02.0-fv02.0",
        timestr = "120000",
    ),

    GAMSSA_ABOM = dict(
        suffix = "ABOM-L4_GHRSST-SSTfnd-GAMSSA_28km-GLOB-v02.0-fv01.0",
        timestr = "120000",
    ),

    DMIOI_DMI= dict(
        suffix = "DMI-L4_GHRSST-SSTfnd-DMI_OI-GLOB-v02.0-fv01.0",
        timestr = "000000",
    ),

    GPBN_OSPO = dict(
        suffix = "OSPO-L4_GHRSST-SSTfnd-Geo_Polar_Blended_Night-GLOB-v02.0-fv01.0",
        timestr = "000000",
    ),

    MUR_JPL = dict(
        suffix = "JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1",
        timestr = "090000",
    ),


)


varnames = {
    "analysed_sst" : "sst",
}

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--input-root', type=str, help='Name of the dataset origin, such as `cwao`, `ecwf`.', required=True)
    parser.add_argument('--output-root', type=str, help='Name of the dataset origin, such as `cwao`, `ecwf`.', required=True)
    parser.add_argument('--date-range', type=str, nargs=2, help="Date range to download", required=True)
    parser.add_argument('--mode', type=str, help="Mode decide whether to do daily or pentad", choices=["daily", "pentad"], required=True)
    parser.add_argument('--datasets', type=str, nargs="+", help="Date range to download", required=True)
    args = parser.parse_args()
    
    print(args)
    
    for dataset in args.datasets:

        print("Doing dataset: ", dataset)


        dataset_detail = dataset_details[dataset]

        input_dir = Path(args.input_root) / dataset
        output_dir = Path(args.output_root) / dataset
   
        output_dir.mkdir(parents=True, exist_ok=True)
 
 
        if args.mode == "pentad":
            print("Not implemented yet for `pentad` mode. ")
        
        elif args.mode == "daily":
            
            ds_backup = None 
            
            d = None
            for dt in pd.date_range(args.date_range[0], args.date_range[1], freq="D", inclusive="left"):
               
                try: 
                    print("Processing datetime = %s" % (dt.strftime("%Y-%m-%d")))
                    
                    input_file = input_dir / "{datestr:s}{timestr:s}-{suffix:s}.nc".format(
                        datestr = dt.strftime("%Y%m%d"),
                        dataset = dataset,
                        timestr = dataset_detail["timestr"],
                        suffix = dataset_detail["suffix"],
                    )
               
                    output_file = data_loader.getFilenameFromDatetime(dataset, dt, root=args.output_root)
                    
                    ds = xr.open_dataset(input_file)
                    
                    first_nonzero_lon = findfirst(ds.coords["lon"] > 0)
                    print("first_nonzero_lon = ", first_nonzero_lon)
                    ds = ds.roll(lon=-first_nonzero_lon, roll_coords=True)

                    dlat = ds.coords["lat"][1] - ds.coords["lat"][0]
                    if dlat < 0:
                        print("dlat < 0. Flip the coord.")
                        ds = ds.isel(lat=slice(None, None, -1))
                   
                    ds = ds.assign_coords(
                        lon = ds.coords["lon"].to_numpy() % 360,
                        lat = ds.coords["lat"].to_numpy(),
                    )
                    
                    print("Output: ", output_file)
                    ds.to_netcdf(
                        output_file,
                        unlimited_dims="time",
                        encoding={'time':{'units':'hours since 1970-01-01'}},
                    )

                except Exception as e:
                    print("Something is wrong with dt = ", str(dt))
                    traceback.print_exc()
 



