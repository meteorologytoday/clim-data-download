import xarray as xr
import os
import pandas as pd

archive_root="./data/data20"

def open_dataset(varset, model_version, start_time):
   
    merge_data = [] 
    # Load control and perturbation
    for ens_type in ["ctl", "pert"]:
        
        save_dir = os.path.join(
            archive_root,
            model_version,
            ens_type,
            varset,
        )
        
        start_time_str = start_time.strftime("%Y_%m-%d")
          
        loading_filename = "{save_dir:s}/ECCC-S2S_{model_version:s}_{ens_type:s}_{varset:s}_{start_time:s}.nc".format(
            save_dir = save_dir,
            model_version = model_version,
            ens_type = ens_type,
            varset = varset,
            start_time = start_time_str,
        )


        ds = xr.open_dataset(loading_filename)
        ds = ds.rename_dims(time="lead_time").rename_vars(time="lead_time").expand_dims(dim={'start_time': [start_time,]}) 
        #print(ds)
        if ens_type == "ctl":
            ds = ds.expand_dims(dim={'number': [0,]}, axis=2)


        #print("### ", ens_type)
        #print(ds)

        merge_data.append(ds)

    ds = xr.merge(merge_data)

    return ds

if __name__ == "__main__":
    
    varset = "Q"
    model_version = "GEPS5"
    start_time = pd.Timestamp("2017-01-03")
    ds = open_dataset(varset, model_version, start_time) 
   
    print(ds) 
