with open("shared_header.py", "rb") as source_file:
    code = compile(source_file.read(), "shared_header.py", "exec")
exec(code)


import traceback
import cdsapi
import numpy as np
import pandas as pd
import xarray as xr
import shutil

c = cdsapi.Client()


dataset_name = "ERA5-derived-daily"

def ifSkip(dt):

    skip = False
#    if not ( dt.month in [10, 11, 12, 1, 2, 3, 4] ):
#        skip = True

    return skip

nproc = 1


varnames = [
#    'geopotential',
#    'u_component_of_wind',
#    'v_component_of_wind',
#    'specific_humidity',

#    '10m_u_component_of_wind',
#    '10m_v_component_of_wind',
#    'mean_sea_level_pressure',
#    '2m_temperature',
#    'surface_sensible_heat_flux',
#    'surface_latent_heat_flux',
#    'surface_net_solar_radiation',
#    'surface_net_thermal_radiation',

#    'sea_surface_temperature',

#    'mean_surface_latent_heat_flux',
#    'mean_surface_sensible_heat_flux',
#    'mean_surface_net_solar_radiation',
#    'mean_surface_net_thermal_radiation',
    "top_net_thermal_radiation",
    "total_precipitation",

#    "convective_precipitation",
#    "convective_rain_rate",
#    "large_scale_precipitation",
#    "large_scale_rain_rate",

]


mapping_longname_shortname = {
    '10m_u_component_of_wind'       : 'u10',
    '10m_v_component_of_wind'       : 'v10',
    'mean_sea_level_pressure'       : 'msl',
    '2m_temperature'                : 't2m',
    'sea_surface_temperature'       : 'sst',
    'surface_sensible_heat_flux'    : 'sshf',
    'surface_latent_heat_flux'      : 'slhf',
    'surface_net_thermal_radiation' : 'str',
    'surface_net_solar_radiation'   : 'ssr',
    'specific_humidity'             : 'q',
    'u_component_of_wind'           : 'u',
    'v_component_of_wind'           : 'v',
    "convective_precipitation"      : 'cp',
    "convective_rain_rate"          : 'crr',
    "large_scale_precipitation"     : 'lsp',
    "large_scale_rain_rate"         : 'lsrr',
    "total_precipitation"           : 'tp',
    "top_net_thermal_radiation"     : 'ttr',
}

var_type = dict(
    
    pressure = [
        'geopotential',
        'u_component_of_wind',
        'v_component_of_wind',
        'specific_humidity',
    ],
    
    surface  = [
        '10m_u_component_of_wind',
        '10m_v_component_of_wind',
        'mean_sea_level_pressure',
        '2m_temperature',
        'sea_surface_temperature',
        'surface_sensible_heat_flux',
        'surface_latent_heat_flux',
        'surface_net_solar_radiation',
        'surface_net_thermal_radiation',
        "convective_precipitation",
        "convective_rain_rate",
        "large_scale_precipitation",
        "large_scale_rain_rate",
        "total_precipitation",
        "top_net_thermal_radiation"
    ],
)


# This is the old version
area = [
    90, -180, -90, 180,
]


full_pressure_levels = [
    '1', '2', '3',
    '5', '7', '10',
    '20', '30', '50',
    '70', '100', '125',
    '150', '175', '200',
    '225', '250', '300',
    '350', '400', '450',
    '500', '550', '600',
    '650', '700', '750',
    '775', '800', '825',
    '850', '875', '900',
    '925', '950', '975',
    '1000',
]

pressure_levels = {
    'geopotential' : ['500', '850', '925', '1000', ],
    'v_component_of_wind' : ['200', '300', '500', '700', '850', '925', '1000'],
    'u_component_of_wind' : ['200', '300', '500', '700', '850', '925', '1000'],
    'specific_humidity' :       ['200', '300', '500', '700', '850', '925', '1000'],
}


   
beg_time = pd.Timestamp(year=beg_time.year, month=beg_time.month, day=beg_time.day)
end_time = pd.Timestamp(year=end_time.year, month=end_time.month, day=end_time.day)

 
download_tmp_dir = os.path.join(archive_root, dataset_name, "tmp")

if os.path.isdir(download_tmp_dir):
    print("Remove temporary directory: ", download_tmp_dir)
    shutil.rmtree(download_tmp_dir)



#
def doJob(t, varname, detect_phase=False):
    # phase \in ['detect', 'work']
    result = dict(dt=t, varname=varname, status="UNKNOWN", need_work=False, detect_phase=detect_phase)

    try:
        y = t.year
        m = t.month
        
        time_ym_str = t.strftime("%Y-%m")
        
        file_prefix = "ERA5"
 
        tmp_filename_downloading = os.path.join(download_tmp_dir, "%s-%s-%s.nc.downloading.tmp" % (file_prefix, varname, time_ym_str,))
        tmp_filename_downloaded  = os.path.join(download_tmp_dir, "%s-%s-%s.nc.downloaded.tmp" % (file_prefix, varname, time_ym_str,))

        month_beg = pd.Timestamp(year=y, month=m, day=1)
        month_end = month_beg + pd.offsets.MonthBegin()


        download_ds = None
        need_work = False 
        # Detecting
        for dt in pd.date_range(month_beg, month_end, freq="D", inclusive="left"):
            
            download_dir = os.path.join(archive_root, dataset_name, varname)
            if not os.path.isdir(download_dir):
                print("Create dir: %s" % (download_dir,))
                Path(download_dir).mkdir(parents=True, exist_ok=True)

            full_time_str = dt.strftime("%Y-%m-%d") 
            output_filename = os.path.join(download_dir, "%s-%s-%s.nc" % (file_prefix, varname, full_time_str, ))

            # First round is just to decide which files
            # to be processed to enhance parallel job 
            # distribution. I use variable `phase` to label
            # this stage.
            if detect_phase is True:
                
                need_work = need_work or ( not os.path.isfile(output_filename) )
                
                continue
                    
            if os.path.isfile(output_filename):
                print("[%s] Data already exists. Skip." % (full_time_str, ))
                continue
            else:
                print("[%s] Now producing file: %s" % (full_time_str, output_filename,))


            # download hourly data is not yet found
            if not os.path.isfile(tmp_filename_downloaded): 

                days_of_month = int((month_end - month_beg) / pd.Timedelta(days=1))
                days_list = [ "%02d" % d for d in range(1, days_of_month+1) ]

                if varname in var_type['pressure']:
                    era5_dataset_name = 'reanalysis-era5-pressure-levels'
                    params = {
                                'product_type': 'reanalysis',
                                'format': 'netcdf',
                                'area': area,
                                'day': days_list,
                                'month': [
                                        "%02d" % m,
                                    ],
                                'year': [
                                        "%04d" % y,
                                    ],
                                'pressure_level': pressure_levels[varname] if varname in pressure_levels else full_pressure_levels,
                                'variable': [varname,],
                    }

                elif varname in var_type['surface']:
                        
                    era5_dataset_name = 'derived-era5-single-levels-daily-statistics'
                    params = {
                                'product_type': 'reanalysis',
                                "daily_statistic": "daily_mean",
                                "time_zone": "utc+00:00",
                                "frequency": "1_hourly",
                                'format': 'netcdf',
                                'area': area,
                                'day': days_list,
                                'month': [
                                        "%02d" % m,
                                    ],
                                'year': [
                                        "%04d" % y,
                                    ],
                                'variable': [varname,],
                    }



                print("Downloading file: %s" % ( tmp_filename_downloading, ))
                c.retrieve(era5_dataset_name, params, tmp_filename_downloaded)
                #c.retrieve(era5_dataset_name, params, tmp_filename_downloading)
                #pleaseRun("ncks -O --mk_rec_dmn time %s %s" % (tmp_filename_downloading, tmp_filename_downloaded,))

                # Open and average with xarray
                if download_ds is None:
                    download_ds = xr.open_dataset(tmp_filename_downloaded)

                    
                sel_time = [ dt ]
                
                print("Select time: ", sel_time)
                shortname = mapping_longname_shortname[varname]

                subset_da = download_ds[shortname].sel(valid_time=sel_time)

                if varname in [
                    "total_precipitation",
                    "convective_precipitation",
                    "large_scale_precipitation",
                ]:
                    subset_da = subset_da.sum(dim="valid_time", keep_attrs=True)
                else:
                    subset_da = subset_da.mean(dim="valid_time", keep_attrs=True)

                subset_da = subset_da.expand_dims(dim="time", axis=0).assign_coords(
                    {"time": [dt,]}
                )
                #pleaseRun("ncra -O -d time,%d,%d %s %s" % (dhr*i, dhr*(i+1)-1, tmp_filename_downloaded, output_filename,))
                subset_da.to_netcdf(output_filename, unlimited_dims="time")
                if os.path.isfile(output_filename):
                    print("[%s] File `%s` is generated." % (time_str, output_filename,))


        if detect_phase is True:
            result['need_work'] = need_work
            result['status'] = 'OK' 
        else:
            for remove_file in [tmp_filename_downloading,]:
                if os.path.isfile(remove_file):
                    print("[%s] Remove file: `%s` " % (time_str, remove_file))
                    os.remove(remove_file)

            result['status'] = 'OK'
            print("[%s] Done. " % (time_str,))

    except Exception as e:

        result['status'] = 'ERROR'
        traceback.print_stack()
        traceback.print_exc()
        print(e)



    return result



print("Going to focus on the following variables:")
for i, varname in enumerate(varnames):
    print("[%02d] %s" % (i, varname))



failed_dates = []
dts = pd.date_range(beg_time.strftime("%Y-%m-%d"), end_time.strftime("%Y-%m-%d"), freq="M", inclusive="both")

input_args = []

for dt in dts:

    y = dt.year
    m = dt.month
    
    time_str = dt.strftime("%Y-%m")

    if ifSkip(dt):
        print("Skip the date: %s" % (time_str,))
        continue

    for varname in varnames:
    
        result = doJob(dt, varname, detect_phase=True)
        
        if result['status'] != 'OK':
            print("[detect] Failed to detect variable `%s` of date %s " % (varname, str(dt)))
        
        if result['need_work'] is False:
            print("[detect] Files all exist for (date, varname) =  (%s, %s)." % (time_str, varname))
        else:
            input_args.append((dt, varname,))
        
print("Create dir: %s" % (download_tmp_dir,))
Path(download_tmp_dir).mkdir(parents=True, exist_ok=True)

with Pool(processes=nproc) as pool:

    results = pool.starmap(doJob, input_args)

    for i, result in enumerate(results):
        if result['status'] != 'OK':
            print('!!! Failed to generate output of date %s.' % (result['dt'].strftime("%Y-%m-%d_%H"), ))

            failed_dates.append(result['dt'])


print("Tasks finished.")

print("Failed dates: ")
for i, failed_date in enumerate(failed_dates):
    print("%d : %s" % (i+1, failed_date.strftime("%Y-%m"),))


print("Done.")
