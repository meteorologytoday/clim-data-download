import pandas as pd
import xarray as xr
import os
import numpy as np


archive_root = os.path.join("S2S", "ERA5")

mapping_longname_shortname = {
    'geopotential'                  : 'z',
    '10m_u_component_of_wind'       : 'u10',
    '10m_v_component_of_wind'       : 'v10',
    'mean_sea_level_pressure'       : 'msl',
    '2m_temperature'                : 't2m',
    'sea_surface_temperature'       : 'sst',
    'specific_humidity'             : 'q',
    'u_component_of_wind'           : 'u',
    'v_component_of_wind'           : 'v',
    'mean_surface_sensible_heat_flux'    : 'msshf',
    'mean_surface_latent_heat_flux'      : 'mslhf',
    'mean_surface_net_long_wave_radiation_flux'  : 'msnlwrf',
    'mean_surface_net_short_wave_radiation_flux' : 'msnswrf',
}

file_prefix = "ERA5-S2S"
def generate_filename(varset, dt, freq):
   
    dt_str = pd.Timestamp(dt).strftime("%Y-%m-%d_%H")

    save_dir = os.path.join(
        archive_root,
        freq,
        varset,
    )
  
    filename = os.path.join(
        save_dir,
        "{file_prefix:s}-{freq:s}-{varset:s}-{time:s}.nc".format(
            file_prefix = file_prefix,
            freq = freq,
            varset = varset,
            time = dt_str,
        )
    )

 
    return filename

def open_dataset(varname_longname, dt, freq):
  
    loading_filename = generate_filename(varname_longname, dt, freq) 
    ds = xr.open_dataset(loading_filename)
    
    return ds    

if __name__ == "__main__":   
    
    date = "2000-01-01"
    
    ds = open_dataset("v_component_of_wind", date, "inst")

    print(ds)
 
