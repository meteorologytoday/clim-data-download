with open("shared_header.py", "rb") as source_file:
    code = compile(source_file.read(), "shared_header.py", "exec")
exec(code)

import pprint
import traceback
import cdsapi
import numpy as np
import pandas as pd
import xarray as xr
import shutil

c = cdsapi.Client()


dataset_name = "ERA5"

def ifSkip(dt):

    skip = False
    #if not ( dt.month in [10, 11, 12, 1, 2, 3, 4] ):
    #    skip = True

    return skip

nproc = 5


download_times = dict(
    inst       = ["00:00",],
    daily_mean = [ "%02d:00" % i for i in range(24) ],
)

download_variables = [
    ('geopotential',        "inst"),
    ('u_component_of_wind', "inst"),
    ('v_component_of_wind', "inst"),
    ('specific_humidity', "inst"),
    ('10m_u_component_of_wind', "inst"),
    ('10m_v_component_of_wind', "inst"),
    ('mean_sea_level_pressure', "inst"),
    ('sea_surface_temperature',                    "daily_mean"),
    ('mean_surface_latent_heat_flux',              "daily_mean"),
    ('mean_surface_sensible_heat_flux',            "daily_mean"),
    ('mean_surface_net_short_wave_radiation_flux', "daily_mean"),
    ('mean_surface_net_long_wave_radiation_flux',  "daily_mean"),
    ('sea_surface_temperature',                    "daily_mean"),
    ('time_mean_sea_surface_temperature',          "daily_mean"),
]

mapping_longname_shortname = {
    'geopotential'                  : 'z',
    '10m_u_component_of_wind'       : 'u10',
    '10m_v_component_of_wind'       : 'v10',
    'mean_sea_level_pressure'       : 'msl',
    '2m_temperature'                : 't2m',
    'sea_surface_temperature'       : 'sst',
    'time_mean_sea_surface_temperature'       : 'avg_tos',
    'specific_humidity'             : 'q',
    'u_component_of_wind'           : 'u',
    'v_component_of_wind'           : 'v',
    'mean_surface_sensible_heat_flux'    : 'msshf',
    'mean_surface_latent_heat_flux'      : 'mslhf',
    'mean_surface_net_long_wave_radiation_flux'  : 'msnlwrf',
    'mean_surface_net_short_wave_radiation_flux' : 'msnswrf',
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
        'time_mean_sea_surface_temperature',
        'mean_surface_sensible_heat_flux',
        'mean_surface_latent_heat_flux',
        'mean_surface_net_short_wave_radiation_flux',
        'mean_surface_net_long_wave_radiation_flux',
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


   
beg_time = pd.Timestamp(year=beg_time.year, month=beg_time.month, day=1)
end_time = pd.Timestamp(year=end_time.year, month=end_time.month, day=1)

 
download_tmp_dir = os.path.join(archive_root, dataset_name, "tmp")

if os.path.isdir(download_tmp_dir):
    print("Remove temporary directory: ", download_tmp_dir)
    shutil.rmtree(download_tmp_dir)




def doJob(job_details, detect_phase=False):
    # phase \in ['detect', 'work']
    result = dict(job_details = job_details, status="UNKNOWN", need_work=False, detect_phase=detect_phase)

    try:

        dt_ym = job_details["dt_ym"]
        var_longname = job_details["var_longname"]
        var_shortname = mapping_longname_shortname[var_longname]
        freq = job_details["freq"]

        y = dt_ym.year
        m = dt_ym.month
        
        time_ym_str = dt_ym.strftime("%Y-%m")
        
        file_prefix = "ERA5-S2S"
 
            
        tmp_filename_download = os.path.join(download_tmp_dir, "{file_prefix:s}-{freq:s}-{varname:s}-{time:s}.nc.download.tmp".format(
            file_prefix = file_prefix,
            freq = freq,
            varname = var_longname,
            time = time_ym_str,
        ))


        month_beg = pd.Timestamp(year=y, month=m, day=1)
        month_end = month_beg + pd.offsets.MonthBegin()


        download_ds = None

        download_dir = os.path.join(archive_root, dataset_name, freq, var_longname)
        if not os.path.isdir(download_dir):
            print("Create dir: %s" % (download_dir,))
            Path(download_dir).mkdir(parents=True, exist_ok=True)

        # Detecting
        dts = pd.date_range(month_beg, month_end, freq="D", inclusive="left")
        for i, dt in enumerate(dts):
            
            full_time_str = "%s" % (dt.strftime("%Y-%m-%d_%H")) 
            output_filename = os.path.join(download_dir, "{file_prefix:s}-{freq:s}-{varname:s}-{time:s}.nc".format(
                file_prefix = file_prefix,
                freq = freq,
                varname = var_longname,
                time = full_time_str,
            ))

            # First round is just to decide which files
            # to be processed to enhance parallel job 
            # distribution. I use variable `phase` to label
            # this stage.
            if detect_phase is True:

                result['need_work'] = not os.path.isfile(output_filename)

                if result['need_work']:
                    result['status'] = 'OK'
                    return result
               
                if i == len(dts) - 1:
                    result['status'] = 'OK'
                    result['need_work'] = False
                    return result

                else: 
                    continue

                    
            if os.path.isfile(output_filename):
                print("[%s] Data already exists. Skip." % (full_time_str, ))
                continue
            else:
                print("[%s] Now producing file: %s" % (full_time_str, output_filename,))

                # download hourly data is not yet found
                if not os.path.isfile(tmp_filename_download): 

                    days_of_month = int((month_end - month_beg) / pd.Timedelta(days=1))
                    days_list = [ "%02d" % d for d in range(1, days_of_month+1) ]

                    download_time = download_times[freq]
 
                    params = {
                                'product_type': 'reanalysis',
                                'format': 'netcdf',
                                'area': area,
                                'time': download_time,
                                'day': days_list,
                                'month': [
                                        "%02d" % m,
                                    ],
                                'year': [
                                        "%04d" % y,
                                    ],
                                'variable': [var_longname,],
                    }

                   
                    if var_longname in var_type['pressure']:
                        era5_dataset_name = 'reanalysis-era5-pressure-levels'
                        params['pressure_level'] =  pressure_levels[var_longname] if var_longname in pressure_levels else full_pressure_levels
                    elif var_longname in var_type['surface']:
                        era5_dataset_name = 'reanalysis-era5-single-levels'
                    else:
                        raise Exception("The given var `%s` (%s) does not belong to any var_type" % (var_longname, var_shortname))

                    print("Downloading file: %s" % ( tmp_filename_download, ))
                    print("Request content: ")
                    pprint.pp(params)
                    c.retrieve(era5_dataset_name, params, tmp_filename_download)

                # Open and average with xarray
                if download_ds is None:
                    download_ds = xr.open_dataset(tmp_filename_download)

                sel_time = None
                if freq == "inst":
                    sel_time = [dt,] 
                elif freq == "daily_mean":
                    sel_time = [ dt + pd.Timedelta(hours=k) for k in range(24) ]
                    
                print("Select time: ", sel_time)
                print("ds = ", download_ds)

                subset_da = download_ds[var_shortname].sel(time=sel_time)

                subset_da = subset_da.mean(dim="time", keep_attrs=True).expand_dims(dim="time", axis=0).assign_coords(
                    {"time": [dt,]}
                )
                
                subset_da.to_netcdf(output_filename, unlimited_dims="time")
                if os.path.isfile(output_filename):
                    print("[%s] File `%s` is generated." % (time_str, output_filename,))


        for remove_file in [tmp_filename_download,]:
            if os.path.isfile(remove_file):
                print("[%s] Remove file: `%s` " % (time_str, remove_file))
                os.remove(remove_file)

        result['status'] = 'OK'

    except Exception as e:

        result['status'] = 'ERROR'
        traceback.print_stack()
        traceback.print_exc()
        print(e)

    print("[%s] Done. " % (time_str,))

    return result



print("Going to focus on the following variables:")
for i, (varname, freq) in enumerate(download_variables):
    print("[%02d] %s %s" % (i, freq, varname))

print("We have the following types of frequency with their download time:")
for i, freq in enumerate(download_times.keys()):
    print("[%02d] %s : %s" % (i, freq, str(download_times[freq])))













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

    for var_longname, freq in download_variables:

        job_details = dict(
            dt_ym = dt,
            var_longname = var_longname,
            freq = freq,
        )
 
        result = doJob(job_details, detect_phase=True)
        
        if result['status'] != 'OK':
            print("[detect] Failed to detect variable `%s` of date %s " % (var_longname, str(dt)))
        
        if result['need_work'] is False:
            print("[detect] Files all exist for (date, var_longname) =  (%s, %s)." % (time_str, var_longname))
        else:
            input_args.append((job_details,))
        
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
