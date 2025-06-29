print("Loading library")
import data_loader
import numpy as np
import xarray as xr
import traceback
from pathlib import Path 
import pandas as pd
import argparse
print("Done")

datatypes = ["mean", "anom"]

doing_varname = dict(
    mean = "sst",
    anom = "anom",
)

varname_mapping = dict(
    sst = "sst",
    anom = "ssta",
)


input_file_fmt = "sst.day.{datatype:s}.{year:04d}.nc"

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--input-root', type=str, help='Name of the dataset origin, such as `cwao`, `ecwf`.', required=True)
    parser.add_argument('--output-root', type=str, help='Name of the dataset origin, such as `cwao`, `ecwf`.', required=True)
    parser.add_argument('--date-range', type=str, nargs=2, help="Date range to download", required=True)
    parser.add_argument('--mode', type=str, help="Mode decide whether to do daily or pentad", choices=["daily", "pentad"], required=True)
    args = parser.parse_args()
    
    print(args)
    
    input_dir = Path(args.input_root) / "oisst"
    output_root = Path(args.output_root)
    
    for datatype in datatypes:
        
        varname = doing_varname[datatype]
        new_varname = varname_mapping[varname]
        
        if args.mode == "pentad":
            print("Not implemented yet for `pentad` mode. ")
        
        elif args.mode == "daily":
            
            
            ds_backup = None 
            for dt in pd.date_range(args.date_range[0], args.date_range[1], freq="D", inclusive="left"):
                
                print("Processing datetime = %s" % (dt.strftime("%Y-%m-%d")))
                
                input_file = input_dir / input_file_fmt.format(
                    year = dt.year,
                    datatype = datatype,
                )
                
                
                ds = xr.load_dataset(input_file)
                
                output_file = data_loader.getFilenameFromDatetime("oisst", dt, root=output_root)
                
                if output_file.exists():
                    print("File %s already exists. Skip." % (str(output_file),))
                    continue
                
                new_data = dict()
                for _varname in [varname, ]:  # Make it into a loop for future flexibility
                   
                    print(ds) 
                    d = np.zeros((1, ds.dims["lat"], ds.dims["lon"]))
                    d[0, :, :] = ds[_varname].sel(time=dt).to_numpy()

                    if varname == "sst":
                        d += 273.15

                    new_data[new_varname] = ( ["time", "lat", "lon"], d )

               
                new_ds = xr.Dataset(
                    data_vars=new_data,
                    coords=dict(
                        time = ( ["time", ] , [dt, ]),
                        lat  = ds.coords["lat"],
                        lon  = ds.coords["lon"],
                    ),
                    attrs=dict(description="Postprocessed OISST data."),
                )

                output_file.parent.mkdir(parents=True, exist_ok=True)

                print("Output: ", output_file)
                new_ds.to_netcdf(
                    output_file,
                    unlimited_dims="time",
                    encoding={'time':{'units':'hours since 1970-01-01'}},
                )

