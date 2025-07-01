from multiprocessing import Pool
import multiprocessing
from pathlib import Path
import traceback
import argparse
#import tempfile

#import cdsapi
import numpy as np
import pandas as pd
import xarray as xr

import TRMM_tools
import earthaccess
import os

from io import BytesIO

def genTRMMFileLink(dt):

    day_beg = pd.Timestamp(year=dt.year, month=dt.month, day=dt.day)
    day_of_year = int( ( day_beg - pd.Timestamp(year=dt.year, month=1, day=1)) / pd.Timedelta(days=1) ) + 1
   
    beg_time = dt
    end_time = dt + pd.Timedelta(minutes=29, seconds=59)
    beg_minute = int( (dt - day_beg ) / pd.Timedelta(minutes=1) )

 
    link = "https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGHH.07/{year:04d}/{day_of_year:03d}/3B-HHR.MS.MRG.3IMERG.{date:s}-S{beg_time:s}-E{end_time:s}.{beg_minute:04d}.V07B.HDF5".format(
        year = dt.year,
        day_of_year = day_of_year,
        date = dt.strftime("%Y%m%d"),
        beg_time = beg_time.strftime("%H%M%S"),
        end_time = end_time.strftime("%H%M%S"),
        beg_minute = beg_minute,
    )

    return link


print("Login earthaccess... ", ending="")
earthaccess.login()
session = earthaccess.get_requests_https_session()
print("Done.")




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


def doJob(details, detect_phase=False):
        
    result = dict(
        details = details,
        status="UNKNOWN",
        need_work=None,
        output_file = None,
    )

    try:

#        lat_rng         = details["lat_rng"]
        dt              = details["dt"]
        hours_skip      = details["hours_skip"]
        archive_root    = details["archive_root"]
        
        output_file = TRMM_tools.essentials.genFilePath(
            dt=dt,
            hours_skip = hours_skip,
            varname="precipitation",
            root=archive_root,
        )
        
        result["output_file"] = output_file
        if detect_phase:
            result["need_work"] = not output_file.exists()
            result["status"] = "OK"
            return result
        
        output_file.parent.mkdir(exist_ok=True, parents=True)
        
        download_dts = pd.date_range(dt, dt + pd.Timedelta(hours=hours_skip), freq=pd.Timedelta(minutes=30))

        #with tempfile.TemporaryDirectory(prefix="TRMM_DATA_%s" % (dt.strftime("%Y-%m-%dT%H:%M:%S"),) ) as tmpdirname:
            
        #    tmpdir = Path(tmpdirname)
        #    print("created temporary directory : %s" % (str(tmpdir),))

        ds = []
        for download_dt in download_dts:
            
            link = genTRMMFileLink(download_dt)
            print("Download link: ", link)
            r = session.get(link)
            ds.append(xr.open_dataset(BytesIO(r.content), engine="h5netcdf", group="/Grid"))


        ds = xr.merge(ds)

        print("Merged ds: ", ds)

        print("Output file: ", output_file)
        ds.to_netcdf(
            output_file,
            unlimited_dims="start_time",
        )

        ds.close()
            
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
    parser.add_argument('--date-range', type=str, nargs=2, help="Date range to download", required=True)
    parser.add_argument('--archive-root', type=str, help='Input directories.', required=True)
    parser.add_argument('--hours-skip', type=int, help="Hours of span", default=3)
    parser.add_argument('--nproc', type=int, help="Number of jobs" , default=1)
    args = parser.parse_args()

    print(args)
    
    date_beg = pd.Timestamp(args.date_range[0])
    date_end = pd.Timestamp(args.date_range[1])

    failed_output_files = []

    dts = pd.date_range(date_beg, date_end, freq=pd.Timedelta(hours=args.hours_skip), inclusive="left")
    input_args = []

    for dt in dts:

        details = dict(
            dt = dt,
            hours_skip   = args.hours_skip,
            archive_root = Path(args.archive_root),
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
