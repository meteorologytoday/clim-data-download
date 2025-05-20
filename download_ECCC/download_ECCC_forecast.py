from multiprocessing import Pool
import multiprocessing
from pathlib import Path
import traceback

#import cdsapi
import numpy as np
import pandas as pd
import xarray as xr
import ECCC_tools
import shutil

from ecmwfapi import ECMWFDataServer
server = ECMWFDataServer()

def pleaseRun(cmd):
    print(">> %s" % cmd)
    os.system(cmd)




g0 = 9.81

file_exists = np.vectorize(os.path.exists)



output_dir_root = os.path.join(archive_root, "data_forecast", "raw")
download_tmp_dir = Path(output_dir_root) / "tmp"
download_tmp_dir.mkdir(parents=True, exist_ok=True)

def ifSkip(dt):


    if dt.month != 9:
        skip = True

    else:
        skip = False

    return skip

nproc = 3


# There are 4 requests to completely download the data in 
# a given time. It is
#
#  1. UVTZ
#  2. Q
#  3. W
#  4. 19 surface layers (inst)
#  5. 9  surface layers (avg)
#  6. 9 2D ocean variables

server.retrieve({
    "class": "s2",
    "dataset": "s2s",
    "date": "2025-01-02/2025-01-06/2025-01-09/2025-01-13/2025-01-16/2025-01-20/2025-01-23/2025-01-27/2025-01-30",
    "expver": "prod",
    "levtype": "sfc",
    "model": "glob",
    "origin": "cwao",
    "param": "34",
    "step": "0-24/24-48/48-72",
    "stream": "enfo",
    "time": "00:00:00",
    "type": "cf",
    "target": "output"
})


def generateRequest(nwp_type, starttime_dts, ens_type, varset, number=None, model_version_date=None):

    # Shared properties
    req = {
        "dataset": "s2s",
        "class": "s2",
        "expver": "prod",
        "model": "glob",
        "origin": "cwao",
        "time": "00:00:00",
        "target": "output"
    }
    
    if nwp_type == "forecast":
        
        req["stream"] = "enfo"
        req["date"] = "/".join([ start_dt.strftime("%Y-%m-%d") for start_dt in starttime_dts ])

    elif nwp_type == "hindcast":
        
        req["stream"] = "enfh"

        # Add in dates
        req["date"] = model_version_dt.strftime("%Y-%m-%d")
        #print([ start_dt.strftime("%Y-%m-%d") for start_dt in starttime_dts ])

        
        req["hdate"] = "/".join([ start_dt.strftime("%Y-%m-%d") for start_dt in starttime_dts ])
        #"date": "2019-09-12",
        #"hdate": "1998-09-12/1999-09-12/2000-09-12/2001-09-12",

    else:
        raise Exception("Unknown nwp_type: %s" % (nwp_type,))

    
    
    # Add in ensemble information
    if ens_type == "ctl":
        
        req["type"] = "cf"

    elif ens_type == "pert":
        
        req["type"] = "pf"

        if number is None:
            if nwp_type == "forecast":
                number = list(range(1, 20))
            elif nwp_type == "hindcast":
                number = list(range(1, 4))
                
        
        req["number"] = "/".join([ "%d" % n for n in number ])
        

    else:
        raise Exception("Unknown ens_type: %s" % (ens_type,))


    # Add in field information

    if varset == "UVTZ":

        # 3D UVTZ
        req.update({
            "levelist": "10/50/100/200/300/500/700/850/925/1000",
            "levtype": "pl",
            "param": "130/131/132/156",
            "step": "24/48/72/96/120/144/168/192/216/240/264/288/312/336/360/384/408/432/456/480/504/528/552/576/600/624/648/672/696/720/744/768",
        })

    elif varset == "W":
        
        # 3D W
        req.update({
            "levelist": "500",
            "levtype": "pl",
            "param": "135",
            "step": "24/48/72/96/120/144/168/192/216/240/264/288/312/336/360/384/408/432/456/480/504/528/552/576/600/624/648/672/696/720/744/768",
        })

    elif varset == "Q":
        
        # 3D Q
        req.update({
            "levelist": "200/300/500/700/850/925/1000",
            "levtype": "pl",
            "param": "133",
            "step": "24/48/72/96/120/144/168/192/216/240/264/288/312/336/360/384/408/432/456/480/504/528/552/576/600/624/648/672/696/720/744/768",
        })

    elif varset == "surf_inst":
        
        # Surface inst (exclude: land-sea mask, orography, min/max 2m temp in the last 6hrs)
        req.update({
            "levtype": "sfc",
            "param": "228228/134/146/147/151/165/166/169/175/176/177/179/174008/228143/228144",
            "step": "24/48/72/96/120/144/168/192/216/240/264/288/312/336/360/384/408/432/456/480/504/528/552/576/600/624/648/672/696/720/744/768",
        })
    
    elif varset == "surf_avg":
        
        # Surface avg
        req.update({
            "levtype": "sfc",
            "param": "31/33/34/136/167/168/228032/228141/228164",
            "step": "0-24/24-48/48-72/72-96/96-120/120-144/144-168/168-192/192-216/216-240/240-264/264-288/288-312/312-336/336-360/360-384/384-408/408-432/432-456/456-480/480-504/504-528/528-552/552-576/576-600/600-624/624-648/648-672/672-696/696-720/720-744/744-768",
        })

    elif varset == "ocn2d_avg":

        # 2D ocean   
        req.update({
            "levtype": "o2d",
            "param": "151126/151131/151132/151145/151163/151175/151219/151225/174098",
            "step": "0-24/24-48/48-72/72-96/96-120/120-144/144-168/168-192/192-216/216-240/240-264/264-288/288-312/312-336/336-360/360-384/384-408/408-432/432-456/456-480/480-504/504-528/528-552/552-576/576-600/600-624/624-648/648-672/672-696/696-720/720-744/744-768",
        })


    return req

def doJob(details, detect_phase=False):
    
    try:

        # phase \in ['detect', 'work']
        result = dict(details = details, status="UNKNOWN", need_work=False, detect_phase=detect_phase)
        
        
        
        Job(details, phasereq, varset, starttime_md, year_group, output_file_group, output_separate_files):

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
            
            # 2025-03-31: The offset should be 0 instead of 24 when it is 
            #             instantaneous. I made a mistake long ago. I should
            #             correct this in the future.
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
            
            # 2025-03-31: The offset should be 0 instead of 24 when it is 
            #             instantaneous. I made a mistake long ago. I should
            #             correct this in the future.
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



if __name__ == "__main__":



    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--nwp-type', type=str, help='Type of NWP. Valid options: `forecast`, `hindcast`.', required=True, choices=["forecast", "hindcast"])
    parser.add_argument('--archive-root', type=str, help='Input directories.', default="ECCC_data")
    parser.add_argument('--date-range', type=str, nargs=2, help="Date range to download", required=True)
    parser.add_argument('--lead-days', type=int, help="How many lead days" , default=32)
    parser.add_argument('--model-versions', type=str, nargs="+")
    parser.add_argument('--hindcast-year-range', type=int, nargs=2, help="If `--nwp-type` is hindcast, then need to download a bunch of years. This option defines the range of years (inclusive on both year).", default=None)
    args = parser.parse_args()

    print(args)
    
    date_beg = pd.Timestamp(args.date_range[0])
    date_end = pd.Timestamp(args.date_range[1])
    number_of_leadtime = args.lead_days

    failed_output_files = []

    # Loop through a year
    beg_year = 1998
    end_year = 2017
    years = np.arange(beg_year, end_year+1)
    download_group_size_by_year = 20
    dts_in_year = pd.date_range("2001-01-01", "2001-12-31", freq="D", inclusive="both")

    year_groups = [
        years[download_group_size_by_year*i:download_group_size_by_year*(i+1)]
        for i in range(int(np.ceil(len(years) / download_group_size_by_year)))
    ]

    print("Year groups: ")
    print(year_groups)
    for i, year_group in enumerate(year_groups):
        print("[%d]" % (i,), year_group)


    input_args = []


    if args.nwp_type == "hindcast":

        for model_version in args.model_versions:

            print("[MODEL VERSION]: ", model_version)

            for dt in dts_in_year:

                model_version_date = ECCC_tools.modelVersionReforecastDateToModelVersionDate(model_version, dt)

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
                    
                    
                    for ens_type in ["pert", "ctl"]:

                        #for varset in ["UVTZ", "W", "Q", "surf_inst", "surf_avg", "ocn2d_avg"]:
                        for varset in ["surf_inst",]:
                            
                            if varset == "ocn2d_avg":

                                # GEPS5 is persistent SST run. There is no ocean model.
                                if model_version == "GEPS5":
                                    continue

                                # According to Dr. Hai Lin, 
                                # ECCC started providing ocean data after 2020-02-06.
                                # Therefore, there is no ocean data prior to that date.
                                if model_version_date < pd.Timestamp("2020-02-06"):
                                    continue
                            
                            output_dir = os.path.join(
                                output_dir_root,
                                model_version,
                                ens_type,
                                varset,
                            )
                            
                            Path(output_dir).mkdir(parents=True, exist_ok=True)
                                    
                            output_file_group = "ECCC-S2S_{model_version:s}_{ens_type:s}_{varset:s}_{year_group_str:s}_{start_time:s}.nc".format(
                                    model_version = model_version,
                                    ens_type = ens_type,
                                    varset = varset,
                                    year_group_str = "%04d-%04d" % (year_group[0], year_group[-1]),
                                    start_time     = "%02d-%02d" % (month, day),
                            )

                            output_separate_files = [                    
                                "{save_dir:s}/ECCC-S2S_{model_version:s}_{ens_type:s}_{varset:s}_{year:04d}_{start_time:s}.nc".format(
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
                            
                           


    with Pool(processes=nproc) as pool:

        results = pool.starmap(doJob, input_args)

        for i, result in enumerate(results):
            if result['status'] != 'OK':
                print('!!! Failed to generate output file %s.' % (",".join(result['output_files'],)))
                failed_dates.append(result['output_file_full'])


    print("Tasks finished.")

    print("Failed output files: ")
    for i, failed_output_file in enumerate(failed_output_files):
        print("%d : %s" % (i+1, failed_output_file,))

    print("Done.")
