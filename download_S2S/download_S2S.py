print("Loading libraries")
import re
import functools
from multiprocessing import Pool
import multiprocessing
from pathlib import Path
import traceback
import argparse

#import cdsapi
import numpy as np
import pandas as pd
import xarray as xr

import S2S_tools
#import ECCC_tools.hindcast
import S2S_tools.forecast

import shutil

from ecmwfapi import ECMWFDataServer
server = ECMWFDataServer()

import os

print("Done")

def parseRanges(input_str):
    numbers = []
    for part in input_str.split(','):
        part = part.strip()
        if '-' in part:
            start, end = map(int, part.split('-'))
            numbers.extend(range(start, end + 1))
        else:
            numbers.append(int(part))
    return numbers


def pleaseRun(cmd):
    print(">> %s" % cmd)
    os.system(cmd)

g0 = 9.81

def ifSkip(dt):

    if dt.month != 9:
        skip = True

    else:
        skip = False

    return skip

# There are 4 requests to completely download the data in 
# a given time. It is
#
#  1. UVTZ
#  2. Q
#  3. W
#  4. 19 surface layers (inst)
#  5. 9  surface layers (avg)
#  6. 9 2D ocean variables

def generateRequest(
    origin,
    nwp_type,
    starttime_dts,
    number_of_leaddays,
    varset,
    numbers,
    model_version_date=None,
    self_defined_data=None,
):


    # Shared properties
    req = {
        "class": "s2",
        "dataset": "s2s",
        "expver": "prod",
        "model": "glob",
        "origin": origin,
        "time": "00:00:00",
        "target": "output"
    }
        
    inst_or_avg = "avg" if re.search(r"_avg$", varset) else "inst"

    if nwp_type == "forecast":
        
        req["stream"] = "enfo"
        req["date"] = "/".join([ start_dt.strftime("%Y-%m-%d") for start_dt in starttime_dts ])

    elif nwp_type == "hindcast":
        
        req["stream"] = "enfh"

        # Add in dates
        req["date"] = model_version_date.strftime("%Y-%m-%d")
        #print([ start_dt.strftime("%Y-%m-%d") for start_dt in starttime_dts ])

        
        req["hdate"] = "/".join([ start_dt.strftime("%Y-%m-%d") for start_dt in starttime_dts ])
        #"date": "2019-09-12",
        #"hdate": "1998-09-12/1999-09-12/2000-09-12/2001-09-12",

    else:
        raise Exception("Unknown nwp_type: %s" % (nwp_type,))

    
    
    # Add in ensemble information

    if 0 in numbers:   # control run
        if len(numbers) != 1:
            raise Exception("Error: number 0 is control run can can only be downloaded by itself.")

        req["type"] = "cf"

    else: # perturbation run
        
        req["type"] = "pf"
        req["number"] = "/".join([ "%d" % n for n in numbers ])
       

    # Determine steps for snapshot and average. Different origins seem
    # to provide different time. For example, ecmf provided snapshot at 
    # forecast time 0 but cwao does not.
    # This part is complicated. Maybe after a while I can wrap it into
    # a function call.
    time_arr = np.arange(number_of_leaddays) * 24
    step_snapshot = ["%d" % s for s in np.arange(number_of_leaddays) * 24]
    step_avg = ["%d-%d" % (s, s+24) for s in np.arange(number_of_leaddays) * 24]
  
    missing_lead_time_idxs = None
 
    if origin == "cwao":
        
        if inst_or_avg == "inst":
            # no hour 0
            step_snapshot = step_snapshot[1:]
            missing_lead_time_idxs = [0,]
        
    if origin == "kwbc": # GEFS
        
        if inst_or_avg == "avg":
            # no first day
            step_avg = step_avg[1:] 
            missing_lead_time_idxs = [0,]

    step_snapshot = "/".join(step_snapshot)
    step_avg = "/".join(step_avg)
 
 
 
    # Add in field information
    if self_defined_data is not None:
        varset = self_defined_data["varset"]
        req.update(self_defined_data["request"])

    elif varset == "UVTZ":

        # 3D UVTZ
        req.update({
            "levelist": "10/50/100/200/300/500/700/850/925/1000",
            "levtype": "pl",
            "param": "130/131/132/156",
            "step": step_snapshot,
        })

    elif varset == "W":
        
        # 3D W
        req.update({
            "levelist": "500",
            "levtype": "pl",
            "param": "135",
            "step": step_snapshot,
        })

    elif varset == "Q":
        
        # 3D Q
        req.update({
            "levelist": "200/300/500/700/850/925/1000",
            "levtype": "pl",
            "param": "133",
            "step": step_snapshot,
        })

    elif varset == "surf_inst":
        
        # Surface inst (exclude: land-sea mask, orography, min/max 2m temp in the last 6hrs)
        req.update({
            "levtype": "sfc",
            "param": "228228/134/146/147/151/165/166/169/175/176/177/179/174008/228143/228144",
            "step": step_snapshot,
        })
    
    elif varset == "surf_avg":
        
        # Surface avg
        req.update({
            "levtype": "sfc",
            #"param": "31/33/34/136/167/168/228032/228141/228164",
            "param": "31/34",
            "step": step_avg,
        })

    elif varset == "ocn2d_avg":

        # 2D ocean   
        req.update({
            "levtype": "o2d",
            "param": "151126/151131/151132/151145/151163/151175/151219/151225/174098",
            "step": step_avg,
        })


    return req, missing_lead_time_idxs

def doJob(details, detect_phase=False):
    
    result = dict(
        details = details,
        status="UNKNOWN",
        need_work=None,
        output_file = None,
    )

    try:

        # phase \in ['detect', 'work']
        model_version_date = details["model_version_date"]
        origin          = details["origin"]
        nwp_type        = details["nwp_type"]
        model_version   = details["model_version"]
        numbers         = details["numbers"]
        varset          = details["varset"]
        start_time      = details["start_time"]
        lead_days       = details["lead_days"]
        archive_root    = details["archive_root"]
        self_defined_data = details["self_defined_data"]
        download_tmp_dir = details["download_tmp_dir"]
       

        output_files = dict()
        for number in numbers: 
            output_file = S2S_tools.essentials.genFilePath(
                origin,
                model_version,
                nwp_type,
                varset,
                start_time,
                number,
                root=archive_root,
            )
            output_files[number] = output_file

            
        result["output_files"] = output_files

        if detect_phase:

            result["need_work"] = not np.all( [ output_file.exists() for _, output_file in output_files.items() ] )
            result["status"] = "OK"
            return result

        req, missing_lead_time_idxs = generateRequest(
            origin,
            nwp_type,
            [start_time,],
            lead_days,
            varset,
            numbers,
            model_version_date=model_version_date,
            self_defined_data=self_defined_data,
        )
        
        for _, output_file in output_files.items():
            output_file.parent.mkdir(exist_ok=True, parents=True)
       
        
 
        # Detect end

        tmp_output_filename = "%s.%s" % ( output_files[list(output_files.keys())[0]].name, ",".join(["%d" % number for number in numbers]) )
        tmp_file  = download_tmp_dir / ("%s.tmp.grib" % (str(tmp_output_filename),))
        tmp_file2  = download_tmp_dir / ("%s.tmp.nc" % (str(tmp_output_filename),))

        # Set download file name
        req.update(dict(target=str(tmp_file)))
        
        print("Downloading file: %s" % ( tmp_file, ))
        server.retrieve(req)
        
        print("Postprocessing...")
        pleaseRun("grib_to_netcdf -o {output:s} {input:s}".format(
            input =  str(tmp_file),
            output = str(tmp_file2),
        ))
        
        # ============ Check if flattened data has correct years =============
        ds = xr.open_dataset(tmp_file2, engine="netcdf4")
        received_dts = ds.coords["time"]
        
        
        print("ds = ", ds)
        
        
        # ====================================================================
        
        # Now change time to lead_time and pre-pend a 
        # start_time dimension
        start_time_da = xr.DataArray(
            data = [ int( (start_time - pd.Timestamp("1970-01-01") ) / pd.Timedelta(hours=1) ) ],
            dims = ["start_time"],
            attrs=dict(
                description="Model start time",
                units="hours since 1970-01-01 00:00:00",
                calendar = "proleptic_gregorian",
            )
        ).rename("start_time")
        
        number_of_leadtime = lead_days
        inst_or_avg = "avg" if re.search(r"_avg$", varset) else "inst"
        offset = 12 if inst_or_avg == "avg" else 0
            

        lead_time_array = 24 * np.arange(number_of_leadtime) + offset
        valid_lead_time_idx = np.arange(len(lead_time_array), dtype=int)

        if missing_lead_time_idxs is not None:
            for missing_lead_time_idx in missing_lead_time_idxs:
                valid_lead_time_idx = valid_lead_time_idx[valid_lead_time_idx != missing_lead_time_idx]
       
        lead_time_array = lead_time_array[valid_lead_time_idx] 
        print("Lead time array: ", lead_time_array) 
        
        lead_time_da = xr.DataArray(
            data=lead_time_array,
            dims=["lead_time",],
            attrs=dict(
                description="Lead time in hours",
                units="hours",
            ),
        ).rename("lead_time")

        lead_time_bnds_da = None
        if inst_or_avg == "avg":
            
            lead_time_bnds = np.zeros((len(lead_time_da), 2), dtype=float)
            lead_time_bnds[:, 0] = lead_time_array - 12
            lead_time_bnds[:, 1] = lead_time_array + 12
            lead_time_bnds_da = xr.DataArray(
                data=lead_time_bnds,
                dims=["lead_time", "bnd"],
                attrs=dict(
                    description="Lead time in hours",
                    units="hours",
                ),
            ).rename("lead_time_bnds")


        ds = ds.rename_dims(dict(time="lead_time")).assign_coords(
            dict(
                lead_time = lead_time_da,
            ) 
        )

        ds = ds.drop_vars('time').expand_dims(
            dim={ "start_time" : start_time_da },
            axis=0
        ).assign_coords( # Need to do this again because expand_dims does not propagate starttime_da attributes
            start_time = start_time_da
        )

        if inst_or_avg == "avg":
            ds = xr.merge([ds, lead_time_bnds_da])


        # Output each ensemble member

        if req["type"] == "cf":
            ds = ds.expand_dims(
                dim={'number': [0,]},
                axis=2,
            )

        for number in numbers:

            output_file = output_files[number]
            print("# Output file: ", output_file)
            ds.sel(number=[number,]).to_netcdf(
                output_file,
                unlimited_dims="start_time",
            )


        ds.close()
            
        for remove_file in [tmp_file, tmp_file2]:
            if os.path.isfile(remove_file):
                os.remove(remove_file)

        result['status'] = 'OK'

    except Exception as e:

        result['status'] = 'ERROR'
        traceback.print_stack()
        traceback.print_exc()
        
        print(e)

    #print("Finish generating files %s " % (", ".join(output_separate_files),))

    return result




if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--origin', type=str, help='Name of the dataset origin, such as `cwao`, `ecwf`.', required=True)
    parser.add_argument('--nwp-type', type=str, help='Type of NWP. Valid options: `forecast`, `hindcast`.', required=True, choices=["forecast", "hindcast"])
    parser.add_argument('--archive-root', type=str, help='Input directories.', default="ECCC_data")
    parser.add_argument('--date-range', type=str, nargs=2, help="Date range to download", required=True)
    parser.add_argument('--varsets', type=str, nargs="+", help="Varsets", required=True)
    parser.add_argument('--lead-days', type=int, help="How many lead days" , default=32)
    parser.add_argument('--model-versions', type=str, nargs="+")
    parser.add_argument('--nproc', type=int, help="Number of jobs" , default=1)
    parser.add_argument('--numbers', type=str, required=True)
    parser.add_argument('--number-batch-size', type=int, default=10)
    args = parser.parse_args()

    print(args)


    numbers = sorted(np.unique(parseRanges(args.numbers)))

    numbers_batches = []
    if numbers[0] == 0:
        numbers_batches.append([0,])
        numbers = numbers[1:]

    while len(numbers) > 0:
        
        if len(numbers) >= args.number_batch_size:
            numbers_batches.append(numbers[0:args.number_batch_size])
            numbers = numbers[args.number_batch_size:]
        else:
            numbers_batches.append(numbers[:])
            break


    print("Divided number batches: ")
    for i, numbers_batch in enumerate(numbers_batches):
        print("[%2d]: " % (i+1,), numbers_batch)


    date_beg = pd.Timestamp(args.date_range[0])
    date_end = pd.Timestamp(args.date_range[1])
    number_of_leadtime = args.lead_days

    output_dir_root = Path(args.archive_root)
    
    download_tmp_dir = output_dir_root / "tmp"
    download_tmp_dir.mkdir(parents=True, exist_ok=True)

    failed_output_files = []

    dts = pd.date_range(date_beg, date_end, freq="D", inclusive="both")
    input_args = []

    for model_version in args.model_versions:

        print("[MODEL VERSION]: ", model_version)
        for dt in dts:
    
            if args.nwp_type == "hindcast":       
                model_version_date = ECCC_tools.hindcast.modelVersionReforecastDateToModelVersionDate(model_version, dt)
                if model_version_date is None:
                    continue
            elif args.nwp_type == "forecast":
                
                model_version_date = dt
               
                print("Test dt: ", dt) 
                # see https://confluence.ecmwf.int/display/S2S/ECCC+Model
                if not S2S_tools.forecast.checkIfModelVersionDateIsValid(args.origin, model_version, dt):
                    continue

            print("The date %s exists on ECMWF database. " % (dt.strftime("%y/%m/%d")))
            
            for numbers_batch in numbers_batches:
                
                #for varset in ["UVTZ", "W", "Q", "surf_inst", "surf_avg", "ocn2d_avg"]:
                for varset in args.varsets:
                    
                    if varset == "ocn2d_avg":

                        # GEPS5 is persistent SST run. There is no ocean model.
                        if model_version == "GEPS5":
                            continue

                        # According to Dr. Hai Lin, 
                        # ECCC started providing ocean data after 2020-02-06.
                        # Therefore, there is no ocean data prior to that date.
                        if model_version_date < pd.Timestamp("2020-02-06"):
                            continue
                    
                    details = dict(
                        model_version_date = model_version_date,
                        origin          = args.origin,
                        nwp_type        = args.nwp_type,
                        model_version   = model_version,
                        numbers         = numbers_batch,
                        varset          = varset,
                        start_time      = dt,
                        lead_days       = args.lead_days,
                        archive_root    = output_dir_root,
                        self_defined_data = None,
                        download_tmp_dir = download_tmp_dir,
                    )


                    result = doJob(details, detect_phase=True)
                    
                    if result["need_work"]:
                        # pick the first starttime_dts to only give month and day
                        input_args.append((details,))
                    else:
                        output_file = str(result["output_file"])
                        print("File %s already exists. Skip." % (output_file,)) 
                    
                   


    with Pool(processes=args.nproc) as pool:

        results = pool.starmap(doJob, input_args)
        for i, result in enumerate(results):
            if result['status'] != 'OK':
                output_file = str(result['output_file'])
                print('!!! Failed to generate output file %s.' % (output_file,))
                failed_dates.append(output_file)


    print("Tasks finished.")

    print("### Failed output files: ###")
    for i, failed_output_file in enumerate(failed_output_files):
        print("%d : %s" % (i+1, failed_output_file,))
    print("############################")

    print("Done.")
