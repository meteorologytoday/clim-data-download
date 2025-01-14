with open("shared_header.py", "rb") as source_file:
    code = compile(source_file.read(), "shared_header.py", "exec")
exec(code)

import time
import traceback
#import cdsapi
import numpy as np
import pandas as pd
import xarray as xr
import ECMWF_tools
import shutil

from ecmwfapi import ECMWFDataServer
server = ECMWFDataServer()

#c = cdsapi.Client()
model_name = "ECMWF"
nproc = 4
beg_year = 2004
end_year = 2022

file_exists = np.vectorize(os.path.exists)

output_dir_root = os.path.join(archive_root, "data", "raw")
download_tmp_dir = os.path.join(output_dir_root, "tmp")
Path(download_tmp_dir).mkdir(parents=True, exist_ok=True)



print("Model name: %s" % (model_name,))
print("Number of processors: %d" % (nproc,))
print("Output directory root: %s" % (output_dir_root,))
print("Download years = %d ~ %d" % (beg_year, end_year,))



def ifSkip(dt):

    skip = False
    #if dt.month != 9:
    #    skip = True

    #else:
    #    skip = False

    return skip




# There is in total 1 request to completely download the data in 
# a given time. It is
#
#  1. 1  surface layers (surf_avg) : SST


def generateRequest(model_version_dt, starttime_dts, ens_type, varset):

    # Shared properties
    req = {
        "dataset": "s2s",
        "class": "s2",
        "expver": "prod",
        "model": "glob",
        "origin": "ecmf",
        "stream": "enfh",
        "time": "00:00:00",
        "target": "output"
    }
     
    # Add in dates
    req["date"] = model_version_dt.strftime("%Y-%m-%d")
    #print([ start_dt.strftime("%Y-%m-%d") for start_dt in starttime_dts ])

    req["hdate"] = "/".join([ start_dt.strftime("%Y-%m-%d") for start_dt in starttime_dts ])
    
    # Example:
    #"date": "2019-09-12",
    #"hdate": "1998-09-12/1999-09-12/2000-09-12/2001-09-12",
    
    # Add in ensemble information
    if ens_type == "ctl":
        
        req["type"] = "cf"
        req["number"] = "1"

    elif ens_type == "pert":
        
        req["type"] = "pf"
        req["number"] = "/".join(["%d" % (i,) for i in range(1, 11)]) # 10 perturbations

    else:
        raise Exception("Unknown ens_type: %s" % (ens_type,))


    # Add in field information

    if varset == "surf_avg":
        
        # Surface avg
        req.update({
            "levtype": "sfc",
            "param": "34",
            "step": "0-24/24-48/48-72/72-96/96-120/120-144/144-168/168-192/192-216/216-240/240-264/264-288/288-312/312-336/336-360/360-384/384-408/408-432/432-456/456-480/480-504/504-528/528-552/552-576/576-600/600-624/624-648/648-672/672-696/696-720/720-744/744-768/768-792/792-816/816-840/840-864/864-888/888-912/912-936/936-960/960-984/984-1008/1008-1032/1032-1056/1056-1080/1080-1104",
        })

    return req






beg_time = pd.Timestamp(year=beg_time.year, month=beg_time.month, day=1)
end_time = pd.Timestamp(year=end_time.year, month=end_time.month, day=1)

number_of_leadtime = 46 

def doJob(req, varset, starttime_md, year_group, output_file_group, output_separate_files):

    result = dict(output_files = output_separate_files, req=req, status="UNKNOWN")

    tmp_file = os.path.join(download_tmp_dir, "%s.grib" % output_file_group)
    tmp_file2 = os.path.join(download_tmp_dir, "%s.nc_flat" % output_file_group)
    tmp_file3 = os.path.join(download_tmp_dir, "%s.nc_before_reorg" % output_file_group)

    req.update(dict(target=tmp_file))
   
    try:


        print("Downloading file: %s" % ( tmp_file, ))
        server.retrieve(req)
    
        pleaseRun("grib_to_netcdf -o {output:s} {input:s}".format(
            input =  tmp_file,
            output = tmp_file2,
        ))

        # ============ Check if flattened data has correct years =============
        ds = xr.open_dataset(tmp_file2, engine="netcdf4")
        received_dts = ds.coords["time"]#.to_numpy()
        
        # First is the number of timesteps
        if len(received_dts) != number_of_leadtime * len(year_group):
            raise Exception("The downloaded data has %d time steps but we are expecting %d time steps" % (
                len(received_dts),
                number_of_leadtime * len(year_group),
            ))
        
        # Second is to check datetime
        if varset in [ "surf_avg", "ocn2d_avg"]:
           fixed_offset = pd.Timedelta(days=0)
        else:
           fixed_offset = pd.Timedelta(days=1)

        for i, test_year in enumerate(year_group):
            
            test_dts = received_dts[i*number_of_leadtime:(i+1)*number_of_leadtime]
            
            expected_dts = np.array([
                (pd.Timestamp(year=test_year, month=starttime_md.month, day=starttime_md.day) + pd.Timedelta(days=j) + fixed_offset).to_datetime64()
                for j in range(number_of_leadtime)
            ])

            if not np.all(test_dts == expected_dts):
                print("test_dts     = ", test_dts)
                print("expected_dts = ", expected_dts)
                raise Exception("The downloaded data do not have correct expecting datetime. The received dts are: %s" % (str(received_years),))

        # ====================================================================

        for i, output_year in enumerate(year_group):
            
            pleaseRun("ncks -O -d time,{begin_idx:d},{end_idx:d} {input:s} {output:s}".format(
                begin_idx = i     * number_of_leadtime,
                end_idx   = (i+1) * number_of_leadtime - 1,
                input = tmp_file2,
                output = tmp_file3,
            ))


            # Now change time to lead_time and pre-pend a 
            # start_time dimension
            starttime = pd.Timestamp(year=output_year, month=starttime_md.month, day=starttime_md.day)
            starttime_da = xr.DataArray(
                data = [ int( (starttime - pd.Timestamp("1970-01-01") ) / pd.Timedelta(hours=1) ) ],
                dims = ["start_time"],
                attrs=dict(
                    description="Model start time",
                    units="hours since 1970-01-01 00:00:00",
                    calendar = "proleptic_gregorian",
                )
            )
                
            offset = 12 if varset in ["surf_avg", "ocn2d_avg"] else 24
            leadtime_da = xr.DataArray(
                data=24 * np.arange(number_of_leadtime) + offset,  # for average variable its at the noon
                dims=["lead_time"],
                attrs=dict(
                    description="Lead time in hours",
                    units="hours",
                ),
            )

            ds = xr.open_dataset(tmp_file3, engine="netcdf4")
            ds = ds.rename_dims(dict(time="lead_time")).assign_coords(
                dict(
                    lead_time = leadtime_da,
                ) 
            )

            ds = ds.drop_vars('time').expand_dims(
                dim={ "start_time" : starttime_da },
                axis=0
            ).assign_coords( # Need to do this again because expand_dims does not propagate starttime_da attributes
                start_time = starttime_da
            )

            ds.to_netcdf(
                output_separate_files[i],
                unlimited_dims="start_time",
            )

            
        for remove_file in [tmp_file, tmp_file2, tmp_file3]:
            if os.path.isfile(remove_file):
                os.remove(remove_file)

        result['status'] = 'OK'

    except Exception as e:

        result['status'] = 'ERROR'
        traceback.print_stack()
        traceback.print_exc()
        
        print(e)

    print("Finish generating files %s " % (", ".join(output_separate_files),))

    return result


failed_output_files = []

# Loop through a year
years = np.arange(beg_year, end_year+1)
download_group_size_by_year = 20
dts_in_year = pd.date_range("2000-01-01", "2000-12-31", freq="D", inclusive="both")
shuffled_idx = np.random.permutation(np.arange(len(dts_in_year)))


year_groups = [
    years[download_group_size_by_year*i:download_group_size_by_year*(i+1)]
    for i in range(int(np.ceil(len(years) / download_group_size_by_year)))
]

print("Year groups: ")
print(year_groups)
for i, year_group in enumerate(year_groups):
    print("[%d]" % (i,), year_group)


input_args = []
#for model_version in ["GEPS5", "GEPS6"]:
for model_version in ["CY48R1",]:# "GEPS6"]:

    print("[MODEL VERSION]: ", model_version)

    for idx in shuffled_idx:
        
        dt = dts_in_year[idx]

        if ifSkip(dt):
            print("Skip this date: ", dt)
            continue
    
        model_version_date = ECMWF_tools.modelVersionReforecastDateToModelVersionDate(model_version, dt)

        if model_version_date is None:
            
            continue
            
        print("The date %s exists on ECMWF database. " % (dt.strftime("%m/%d")))

        for year_group in year_groups:
            
            month = dt.month
            day = dt.day
            starttime_dts = [
                pd.Timestamp(year=y, month=month, day=day)
                for y in year_group
            ]
            
            if len(starttime_dts) != len(year_group):
                print(starttime_dts)
                print(year_group)
                raise Exception("Weird. Check.")
            
            
            #for ens_type in ["ctl", ]:
            for ens_type in ["ctl", "pert"]:

                #for varset in ["UVTZ", "W", "Q", "surf_inst", "surf_avg", "ocn2d_avg"]:
                for varset in ["surf_avg",]:
                    
                    output_dir = os.path.join(
                        output_dir_root,
                        model_version,
                        ens_type,
                        varset,
                    )
                    
                    Path(output_dir).mkdir(parents=True, exist_ok=True)
                            
                    output_file_group = "{model_name:s}-S2S_{model_version:s}_{ens_type:s}_{varset:s}_{year_group_str:s}_{start_time:s}.nc".format(
                            model_name = model_name,
                            model_version = model_version,
                            ens_type = ens_type,
                            varset = varset,
                            year_group_str = "%04d-%04d" % (year_group[0], year_group[-1]),
                            start_time     = "%02d-%02d" % (month, day),
                    )

                    output_separate_files = [                    
                        "{save_dir:s}/{model_name:s}-S2S_{model_version:s}_{ens_type:s}_{varset:s}_{year:04d}_{start_time:s}.nc".format(
                            model_name = model_name,
                            save_dir = output_dir, 
                            model_version = model_version,
                            ens_type = ens_type,
                            varset = varset,
                            year = _year,
                            start_time     = "%02d-%02d" % (month, day),
                        )
                        for _year in year_group
                    ]
                    
                    
                    #output_file_full = os.path.join(output_dir, output_file)
                    
                    
                    
                    if np.all(file_exists(output_separate_files)):
                        
                        print("Files %s already exist. Skip it." % (", ".join(output_separate_files),))
                        continue
                            
                    req = generateRequest(model_version_date, starttime_dts, ens_type, varset)
                    # pick the first starttime_dts to only give month and day
                    input_args.append((req, varset, starttime_dts[0], year_group, output_file_group, output_separate_files))
                    
                   

failed_dates=[]
with Pool(processes=nproc) as pool:

    results = pool.starmap(doJob, input_args)

    for i, result in enumerate(results):
        if result['status'] != 'OK':
            print('!!! Failed to generate output file %s.' % (",".join(result['output_files'],)))
            failed_dates.append(result['output_files'])


print("Tasks finished.")

print("Failed output files: ")
for i, failed_output_file in enumerate(failed_output_files):
    print("%d : %s" % (i+1, failed_output_file,))

print("Done.")
