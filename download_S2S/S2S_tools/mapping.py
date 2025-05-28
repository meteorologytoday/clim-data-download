import pandas as pd
import xarray as xr
import os
import numpy as np
from pathlib import Path

longshortname_mapping = {
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


