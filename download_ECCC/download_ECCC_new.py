from multiprocessing import Pool
import multiprocessing
from pathlib import Path
import traceback
import argparse

#import cdsapi
import numpy as np
import pandas as pd
import xarray as xr

import ECCC_tools
import ECCC_tools.hindcast
import ECCC_tools.forecast

import shutil

from ecmwfapi import ECMWFDataServer
server = ECMWFDataServer()

import os

def genTmpFileByYearGroup(model_version, ens_type, varset, year_group, start_time_dt):

    return "ECCC-S2S_{model_version:s}_{ens_type:s}_{varset:s}_{year_group_str:s}_{start_time:s}.nc".format(
            model_version = model_version,
            ens_type = ens_type,
            varset = varset,
            year_group_str = "%04d-%04d" % (year_group[0], year_group[-1]),
            start_time     = start_time_dt.strftime("%m-%d"),
    )



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
    nwp_type,
    starttime_dts,
    ens_type,
    varset,
    number=None,
    model_version_date=None,
):

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
        req["date"] = model_version_date.strftime("%Y-%m-%d")
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
    
    result = dict(
        details = details,
        status="UNKNOWN",
        need_work=None,
        output_file = None,
    )

    try:

        # phase \in ['detect', 'work']
        model_version_date = details["model_version_date"]
        nwp_type        = details["nwp_type"]
        rawpost         = details["rawpost"]
        model_version   = details["model_version"]
        ens_type        = details["ens_type"]
        varset          = details["varset"]
        start_time      = details["start_time"]
        lead_days       = details["lead_days"]
        archive_root    = details["archive_root"]
        
        
               
        output_file = ECCC_tools.essentials.genFilePath(
            model_version,
            nwp_type,
            rawpost,
            varset,
            start_time,
            root=archive_root,
            ens_type=ens_type,
        )
        
        result["output_file"] = output_file
        if detect_phase:
            result["need_work"] = not output_file.exists()
            result["status"] = "OK"
            return result

        req = generateRequest(nwp_type, [start_time,], ens_type, varset, model_version_date=model_version_date)
        output_file.parent.mkdir(exist_ok=True, parents=True)

        # Detect end
        tmp_file  = download_tmp_dir / ("%s.tmp.grib" % (str(output_file.name),))
        tmp_file2  = download_tmp_dir / ("%s.tmp.nc" % (str(output_file.name),))

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
        
        # Second is to check datetime
        if varset in [ "surf_avg", "ocn2d_avg"]:
           fixed_offset = pd.Timedelta(days=0)
        else:
           fixed_offset = pd.Timedelta(days=1)

       
        """ 
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
        """

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
        )
        
        # 2025-03-31: The offset should be 0 instead of 24 when it is 
        #             instantaneous. I made a mistake long ago. I should
        #             correct this in the future.
    
        number_of_leadtime = lead_days
        offset = 12 if varset in ["surf_avg", "ocn2d_avg"] else 24
        lead_time_da = xr.DataArray(
            data=24 * np.arange(number_of_leadtime) + offset,  # for average variable its at the noon
            dims=["lead_time"],
            attrs=dict(
                description="Lead time in hours",
                units="hours",
            ),
        )

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


        print("Output file: ", output_file)
        ds.to_netcdf(
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
    parser.add_argument('--nwp-type', type=str, help='Type of NWP. Valid options: `forecast`, `hindcast`.', required=True, choices=["forecast", "hindcast"])
    parser.add_argument('--archive-root', type=str, help='Input directories.', default="ECCC_data")
    parser.add_argument('--date-range', type=str, nargs=2, help="Date range to download", required=True)
    parser.add_argument('--varsets', type=str, nargs="+", help="Varsets", required=True)
    parser.add_argument('--lead-days', type=int, help="How many lead days" , default=32)
    parser.add_argument('--model-versions', type=str, nargs="+")
    parser.add_argument('--nproc', type=int, help="Number of jobs" , default=1)
    args = parser.parse_args()

    print(args)
    
    date_beg = pd.Timestamp(args.date_range[0])
    date_end = pd.Timestamp(args.date_range[1])
    number_of_leadtime = args.lead_days

    output_dir_root = Path(args.archive_root) / ("data_%s" % args.nwp_type) / "raw"
    
    download_tmp_dir = output_dir_root / "tmp"
    download_tmp_dir.mkdir(parents=True, exist_ok=True)

    failed_output_files = []

    rawpost = "raw"

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
                # see https://confluence.ecmwf.int/display/S2S/ECCC+Model


                if not ECCC_tools.forecast.checkIfModelVersionDateIsValid(model_version, dt):
                    continue


                days_of_week = []
                if model_version == "GEPS8":
                    days_of_week = [0, 3] # Mon and Thu
                elif model_version in ["GEPS5", "GEPS6", "GEPS7"]:
                    days_of_week = [3, ] # Thu only
                
                if dt.weekday() not in days_of_week:
                    continue
             
            print("The date %s exists on ECMWF database. " % (dt.strftime("%m/%d")))
            
            for ens_type in ["ctl", "pert"]:
                
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
                        nwp_type        = args.nwp_type,
                        model_version   = model_version,
                        rawpost         = rawpost,
                        ens_type        = ens_type,
                        varset          = varset,
                        start_time      = dt,
                        lead_days       = args.lead_days,
                        archive_root    = Path(args.archive_root),
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
