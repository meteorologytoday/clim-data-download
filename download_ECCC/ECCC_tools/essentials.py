import pandas as pd
import xarray as xr
import os
import numpy as np
from pathlib import Path

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


def open_dataset(nwp_type, model_version, rawpost, varset, start_time, archive_root="."):
  
    if nwp_type == "hindcast" and model_version[:8] == "GEPS6sub":
        model_version = "GEPS6"
 
    if rawpost == "postprocessed":
        
        loading_filename = genFilePath(
            model_version,
            nwp_type,
            rawpost,
            varset,
            start_time,
            root = archive_root,
            ens_type=None
        )

        ds = xr.open_dataset(loading_filename)

    elif rawpost == "raw":
        merge_data = []
 
        # Load control and perturbation
        for ens_type in ["ctl", "pert"]:
 
            loading_filename = genFilePath(
                model_version,
                nwp_type,
                rawpost,
                varset,
                start_time,
                root = archive_root,
                ens_type=ens_type,
            )
            
            
            ds = xr.open_dataset(loading_filename)
            
            if ens_type == "ctl":
                ds = ds.expand_dims(dim={'number': [0,]}, axis=2)

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


def genFilePath(model_version, nwp_type, rawpost, varset, start_time, root = ".", ens_type=None):

    root = Path(root)

    if rawpost == "raw":
        if ens_type not in ["ctl", "pert"]:
            raise Exception("Rawpost is `raw`, then `ens_type` must be given as `ctl` or `pert`.")

        ens_type_str = ".%s" % ens_type
    else:
        ens_type_str = ""

    return root / model_version / nwp_type / rawpost / varset / "ECCC-S2S_{model_version:s}_{nwp_type:s}_{varset:s}_{start_time:s}{ens_type:s}.nc".format(
        model_version = model_version,
        varset = varset,
        start_time  = start_time.strftime("%Y_%m-%d"),
        ens_type = ens_type_str,
        nwp_type = nwp_type,
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
 
