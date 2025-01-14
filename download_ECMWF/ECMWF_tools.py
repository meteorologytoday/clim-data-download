import pandas as pd
import xarray as xr
import os
import numpy as np

model_name = "ECMWF"
archive_root = "./ECMWF_data/data"
model_versions = ["CY48R1", "CY47R3", ]

# The bounds are included
model_version_date_bounds = dict(
    CY48R1 = [ pd.Timestamp("2023-06-27"), pd.Timestamp("2030-06-27"), ],
    CY47R3 = [ pd.Timestamp("2021-10-13"), pd.Timestamp("2023-06-26"), ],
)

# Load the data
valid_model_version_dates = {
    model_version : [] for model_version in model_versions
}

with open("model_version_dates.txt", "r") as f:
    for s in f.readlines():
        if s != "":
            ts = pd.Timestamp(s)
            for model_version in model_versions:
                bnds = model_version_date_bounds[model_version]
                if ts >= bnds[0] and ts <= bnds[1]:
                    valid_model_version_dates[model_version].append(ts)

            
for model_version in model_versions:
    valid_model_version_dates[model_version].sort(key=lambda ts: (ts.month, ts.day))


def printValidModelVersionDates():
     for model_version in model_versions:
        for i, model_version_date in enumerate(valid_model_version_dates[model_version]):
            print("[%s - %d] %s " % (model_version, i, model_version_date.strftime("%Y-%m-%d")))


ECMWF_longshortname_mapping = {
    "geopotential" : "gh",
    "sea_surface_temperature" : "sst",
    'mean_surface_sensible_heat_flux'    : 'msshf',
    'mean_surface_latent_heat_flux'      : 'mslhf',
    "surface_pressure": "sp",
    "mean_sea_level_pressure": "msl",
    "10m_u_component_of_wind":  "u10",
    "10m_v_component_of_wind":  "v10",
    "IVT_x":  "IVT_x",
    "IVT_y":  "IVT_y",
    "IVT":  "IVT",
    "IWV":  "IWV",
}

mapping_varset_varname = {
    'Q' : ['Q',],
    'UVTZ' : ['U',],
    'AR' : ['IVT', 'IVT_x', 'IVT_y', 'IWV',],
    'surf_inst' : ["sshf", "slhf", "ssr", "ssrd", "str", "strd", "ttr",],
    'hf_surf_inst' : ["msshf", "mslhf", "mssr", "mssrd", "mstr", "mstrd", "mttr",],
}

mapping_varname_varset = {}
for varset, varnames in mapping_varset_varname.items():
    for varname in varnames:
        mapping_varname_varset[varname] = varset




"""
    This function receives model_version GEPS5 or GEPS6
    and reforecast date.

    The function returns model_version_date
"""
def modelVersionReforecastDateToModelVersionDate(model_version, reforecast_date):
    
    # IMPORTANT:
    # Notice that there is no two reforecast_date of the same model_version
    # map to two model_version_date
    # The model_version_date is unique with the same model_version
    # Therefore, a specific month-day will map to a specific model_version_date
    # for either GEPS5 or GEPS6

    _valid_model_version_dates = valid_model_version_dates[model_version]
    for i, valid_model_version_date in enumerate(_valid_model_version_dates):
        if valid_model_version_date.month == reforecast_date.month and \
           valid_model_version_date.day   == reforecast_date.day:
            
            return valid_model_version_date
            
        
    return None



def open_dataset(rawpost, varset, model_version, start_time):
  
    if rawpost == "postprocessed":
        
        start_time_str = start_time.strftime("%Y_%m-%d")
 
        save_dir = os.path.join(
            archive_root,
            rawpost,
            model_version,
            varset,
        )
      
        loading_filename = "{save_dir:s}/{model_name:s}-S2S_{model_version:s}_{varset:s}_{start_time:s}.nc".format(
            model_name = model_name,
            save_dir = save_dir,
            model_version = model_version,
            varset = varset,
            start_time = start_time_str,
        )

        ds = xr.open_dataset(loading_filename)

       
    elif rawpost == "raw":
        merge_data = [] 
        # Load control and perturbation
        for ens_type in ["ctl", "pert"]:
            
            save_dir = os.path.join(
                archive_root,
                rawpost,
                model_version,
                ens_type,
                varset,
            )
            
            start_time_str = start_time.strftime("%Y_%m-%d")
              
            loading_filename = "{save_dir:s}/{model_name:s}-S2S_{model_version:s}_{ens_type:s}_{varset:s}_{start_time:s}.nc".format(
                model_name = model_name,
                save_dir = save_dir,
                model_version = model_version,
                ens_type = ens_type,
                varset = varset,
                start_time = start_time_str,
            )


            ds = xr.open_dataset(loading_filename)
            #ds = ds.rename_dims(time="lead_time").rename_vars(time="lead_time").expand_dims(dim={'start_time': [start_time,]}) 

            #print(ds)
            if ens_type == "ctl":
                ds = ds.expand_dims(dim={'number': [0,]}, axis=2)


            #print("### ", ens_type)
            #print(ds)
            
            ds = ds.isel(latitude=slice(None, None, -1))

            merge_data.append(ds)

        ds = xr.merge(merge_data)

    else:

        raise Exception("Unknown rawpost value. Only accept: `raw` and `postprocessed`.")

    # Finally flip latitude
    lat = ds.coords["latitude"]
    if np.all( (lat[1:] - lat[:-1]) < 0 ):
        print("Flip latitude so that it is monotonically increasing")
        ds = ds.isel(latitude=slice(None, None, -1))

    return ds


if __name__ == "__main__":   

    # Part I: model dates and such
    print("Part I: Test model dates finding model version date")

    printValidModelVersionDates()

    test_dates = dict(
        CY48R1 = ["2005-01-03", "2018-02-05", ],
    )

    for model_version, _test_dates in test_dates.items():
        for _test_date in _test_dates:
            _test_date_ts = pd.Timestamp(_test_date)
            print("[%s] Test the date %s maps to model version date " % (model_version, _test_date,), 
                modelVersionReforecastDateToModelVersionDate(model_version, _test_date_ts),
            )
    
    
    # Part II: Loading model data
    
    print("Part II: Test loading model data...")
    print("Current package's global variable `archive_root` = '%s'" % (archive_root,))
    varset = "surf_avg"
    model_version = "CY48R1"
    start_time = pd.Timestamp("2017-01-04")
    step = slice(0, 5)

    ds = open_dataset("raw", varset, model_version, start_time) 
    print(ds) 
 
