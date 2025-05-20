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
frequencies = ["6_hourly", ]#"1_hourly"]
def ifSkip(dt):

    skip = False
#    if not ( dt.month in [10, 11, 12, 1, 2, 3, 4] ):
#        skip = True

    return skip

nproc = 10
max_attempts = 5

varnames = [
#    'geopotential',
#    'u_component_of_wind',
#    'v_component_of_wind',
#    'specific_humidity',

    '10m_u_component_of_wind',
    '10m_v_component_of_wind',
#    'mean_sea_level_pressure',
#    "top_net_thermal_radiation",
#    "total_precipitation",

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
    40, -180, -40, 180,
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


   
download_tmp_dir = os.path.join(archive_root, dataset_name, "tmp")

if os.path.isdir(download_tmp_dir):
    print("Remove temporary directory: ", download_tmp_dir)
    shutil.rmtree(download_tmp_dir)



#
def doJob(details):
    # phase \in ['detect', 'work']

    year  = details["year"]
    month = details["month"]
    frequency = details["frequency"]
    detect_phase = details["detect_phase"]
    varname = details["varname"]
    max_attempts = details["max_attempts"]
    
    label = f"{year:04d}-{month:04d}-{frequency:s}-{varname:s}"
    result = None

    for attempt in range(1, max_attempts+1):

        print(f"[{label:s}] attempt = {attempt:d}")
            
        result = dict(details = details, status="UNKNOWN", need_work=False)

        try:
            month_beg = pd.Timestamp(year=year, month=month, day=1)
            nextmonth_beg = month_beg + pd.offsets.MonthBegin()
            dts = list( pd.date_range(month_beg, nextmonth_beg, freq="D", inclusive="left") ) 
            
            time_str = "%04d-%02d" % (year, month)
            
            file_prefix = "ERA5-derived-daily"
     
            tmp_filename_downloading = os.path.join(download_tmp_dir, "%s-%s-%s-%s.nc.downloading.tmp" % (file_prefix, frequency, varname, time_str,))
            tmp_filename_downloaded  = os.path.join(download_tmp_dir, "%s-%s-%s-%s.nc.downloaded.tmp" % (file_prefix, frequency, varname, time_str,))

            download_ds = None
            need_work = False 
           
                
            download_dir = os.path.join(archive_root, dataset_name, frequency, varname)
            if not os.path.isdir(download_dir):
                print("Create dir: %s" % (download_dir,))
                Path(download_dir).mkdir(parents=True, exist_ok=True)

            
            full_time_strs = [ dt.strftime("%Y-%m-%d") for dt in dts ] 
            output_filenames = [
                os.path.join(download_dir, "%s-%s-%s.nc" % (file_prefix, varname, full_time_str))
                for full_time_str in full_time_strs
            ]
            

            # First round is just to decide which files
            # to be processed to enhance parallel job 
            # distribution. I use variable `phase` to label
            # this stage.
            if detect_phase is True:
                result['need_work'] = not np.all(np.array([
                    os.path.isfile(filename)
                    for filename in output_filenames
                ]))
     
                result['status'] = 'OK' 
              
                return result 
                   

            print(f"Some files do not exist for ({year:04d}-{frequency:s}-{varname:s}). Download a whole year of data now.") 


            months_list = ["%02d" % month ]
            days_list = [ "%02d" % d for d in range(1, 32) ]

            if varname in var_type['pressure']:
                era5_dataset_name = 'reanalysis-era5-pressure-levels'
                params = {
                            'product_type': 'reanalysis',
                            'format': 'netcdf',
                            'area': area,
                            'day': days_list,
                            'month': months_list,
                            'year': [
                                    "%04d" % year,
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
                            "frequency": frequency,
                            'format': 'netcdf',
                            'area': area,
                            'day': days_list,
                            'month': months_list,
                            'year': [
                                    "%04d" % year,
                                ],
                            'variable': [varname,],
                }



            print("Downloading file: %s" % ( tmp_filename_downloading, ))
            c.retrieve(era5_dataset_name, params, tmp_filename_downloaded)

            # Open and average with xarray
            download_ds = xr.open_dataset(tmp_filename_downloaded, engine="netcdf4")


            for dt, output_filename in zip(dts, output_filenames):

                sel_time = [ dt ]
                
                print("Select time: ", sel_time)
                shortname = mapping_longname_shortname[varname]
                subset_da = download_ds[shortname].sel(valid_time=sel_time)
                subset_da.to_netcdf(output_filename, unlimited_dims="time")
                if os.path.isfile(output_filename):
                    print("[%s] File `%s` is generated." % (time_str, output_filename,))

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

        if result["status"] == 'OK':
            break
        else:
            if attempt < max_attempts:
                print("Something went wrong. Retry. ")
            else:
                print("Something went wrong and attempt_max reached.")
                
    return result



print("Going to focus on the following variables:")
for i, varname in enumerate(varnames):
    print("[%02d] %s" % (i, varname))



failed_dates = []

input_args = []

for frequency in frequencies:
    for year in range(year_beg, year_end+1):
        for month in range(1, 13):
            for varname in varnames:
           
                details = dict(
                    year = year,
                    month = month,
                    varname = varname,
                    frequency = frequency,
                    detect_phase = True,
                    max_attempts = max_attempts,
                )
         
                result = doJob(details)
                time_str= f"{year:04d}-{month:02d} ({frequency:s})"
                if result['status'] != 'OK':
                    print("[detect] Failed to detect variable `%s` of %s" % (varname, time_str))
                
                if result['need_work'] is False:
                    print("[detect] Files all exist for variable `%s` of %s" % (varname, time_str))
                else:

                    details["detect_phase"] = False

                    input_args.append((details,))
                
print("Create dir: %s" % (download_tmp_dir,))
Path(download_tmp_dir).mkdir(parents=True, exist_ok=True)

with Pool(processes=nproc) as pool:

    results = pool.starmap(doJob, input_args)

    for i, result in enumerate(results):
        if result['status'] != 'OK':
            print('!!! Failed to generate output of date %d.' % (result['details']['year'], ))

            failed_dates.append(result['details'])


print("Tasks finished.")

#print("Failed dates: ")
#for i, failed_date in enumerate(failed_dates):
#    print("%d (%s): %s" % (i+1, )


print("Done.")
