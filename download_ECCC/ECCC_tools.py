import pandas as pd
import xarray as xr
import os
import numpy as np
from pathlib import Path

archive_root="./ECCC_data/data20_20240723"

model_versions = ["GEPS5", "GEPS6", ]

# The bounds are included
model_version_date_bounds = dict(
    GEPS5 = [ pd.Timestamp("2018-09-27"), pd.Timestamp("2019-06-27"), ],
    GEPS6 = [ pd.Timestamp("2019-07-24"), pd.Timestamp("2021-11-25"), ],
)

valid_start_time_bnds = dict(
    GEPS5 = [ pd.Timestamp("1998-01-01"), pd.Timestamp("2017-12-31")],
    GEPS6 = [ pd.Timestamp("1998-01-01"), pd.Timestamp("2017-12-31")],
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



number_of_GEPS6subX = 2
GEPS6subX_file_fmt = "model_version_dates_GEPS6sub%d.txt"
GEPS6subX_dataset_name_fmt = "GEPS6sub%d"

for sub in range(number_of_GEPS6subX):
    dataset_name = GEPS6subX_dataset_name_fmt % (sub+1,)
    model_versions.append(dataset_name)

test_files = [ os.path.isfile( GEPS6subX_file_fmt % (sub+1,) ) for sub in range(number_of_GEPS6subX) ]
if not np.all(test_files):

    print("Some GEPS6subX does not exist. Reproduce them.")
    print("Generating GEPS6subX...")

    GEPS6_dates = valid_model_version_dates["GEPS6"]
    GEPS6_dates_cnt = np.zeros( (len(GEPS6_dates),) , dtype=int)
    dts_GEPS6_change_year = [ pd.Timestamp(year=2000, month=dt_GEPS6.month, day=dt_GEPS6.day) for dt_GEPS6 in GEPS6_dates ]

    for sub in range(number_of_GEPS6subX):
    

        dataset_name = GEPS6subX_dataset_name_fmt % (sub + 1,)
        GEPS6subX_file = GEPS6subX_file_fmt % (sub + 1,)
        GEPS6subX_valid_dates = []
            
        # Subsampling GEPS6 to generate GEPS6subX
        for i, dt_GEPS5 in enumerate(valid_model_version_dates["GEPS5"]):

            dt_GEPS5_change_year = pd.Timestamp(year=2000, month=dt_GEPS5.month, day=dt_GEPS5.day)

            time_diff = [ dt_GEPS6_change_year - dt_GEPS5_change_year for dt_GEPS6_change_year in dts_GEPS6_change_year ]
            dist = np.argmin( np.abs(time_diff) ) 
            GEPS6_idx = np.argmin( np.abs(time_diff) ) + sub

            if GEPS6_idx >= len(GEPS6_dates):
                GEPS6_idx = len(GEPS6_dates)-1
                
            mapped_GEPS6_dt = GEPS6_dates[GEPS6_idx]
            GEPS6subX_valid_dates.append( mapped_GEPS6_dt )
            GEPS6_dates_cnt[GEPS6_idx] += 1

            print("[%02d] Target GEPS5 time: %s => %s in GEPS6 " % (
                i,
                dt_GEPS5.strftime("%Y-%m-%d"),
                mapped_GEPS6_dt.strftime("%Y-%m-%d"),
            ) )

        print("Writing file: ", GEPS6subX_file)        
        with open(GEPS6subX_file, "w") as f:
            
            for dt in GEPS6subX_valid_dates:
                f.write(dt.strftime("%Y-%m-%d"))
                f.write("\n")

    print("Count of GEPS6_dates being mapped: ", GEPS6_dates_cnt)

for sub in range(number_of_GEPS6subX):
    
    dataset_name = GEPS6subX_dataset_name_fmt % (sub + 1,)
    GEPS6subX_file = GEPS6subX_file_fmt % (sub + 1,)
    GEPS6subX_valid_dates = []

    print("Loading file: ", GEPS6subX_file)
    
    with open(GEPS6subX_file, "r") as f:
        for s in f.readlines():
            if s != "":
                ts = pd.Timestamp(s)
                GEPS6subX_valid_dates.append(ts)

    valid_model_version_dates[dataset_name] = GEPS6subX_valid_dates


   
def printValidModelVersionDates():
     for model_version in model_versions:
        for i, model_version_date in enumerate(valid_model_version_dates[model_version]):
            print("[%s - %d] %s " % (model_version, i, model_version_date.strftime("%Y-%m-%d")))


ECCC_longshortname_mapping = {
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
  
    if model_version[:8] == "GEPS6sub":
        model_version = "GEPS6"
 
    if rawpost == "postprocessed":
        
        start_time_str = start_time.strftime("%Y_%m-%d")
 
        save_dir = os.path.join(
            archive_root,
            rawpost,
            model_version,
            varset,
        )
      
        loading_filename = "{save_dir:s}/ECCC-S2S_{model_version:s}_{varset:s}_{start_time:s}.nc".format(
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
              
            loading_filename = "{save_dir:s}/ECCC-S2S_{model_version:s}_{ens_type:s}_{varset:s}_{start_time:s}.nc".format(
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


def genFilePath(model_version, ens_type, varset, start_time, root = "."):

    root = Path(root)


    return root / model_version / ens_type / varset / "ECCC-S2S_{model_version:s}_{ens_type:s}_{varset:s}_{start_time:s}.nc".format(
        model_version = model_version,
        ens_type = ens_type,
        varset = varset,
        start_time  = start_time.strftime("%Y_%m-%d"),
    )


if __name__ == "__main__":   

    # Part I: model dates and such
    print("Part I: Test model dates finding model version date")

    printValidModelVersionDates()

    test_dates = dict(
        GEPS5 = ["2001-01-03", "2016-02-05", ],
        GEPS6 = ["2012-09-08", "1998-05-06", ],
        GEPS6sub1 = ["2012-03-19", "1998-10-03", ],
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
    varset = "Q"
    model_version = "GEPS5"
    start_time = pd.Timestamp("2017-01-03")
    step = slice(0, 5)

    ds = open_dataset(varset, model_version, start_time) 
    print(ds) 
 
