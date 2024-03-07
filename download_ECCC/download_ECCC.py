with open("shared_header.py", "rb") as source_file:
    code = compile(source_file.read(), "shared_header.py", "exec")
exec(code)


import traceback
import cdsapi
import numpy as np
import pandas as pd
import xarray as xr
import tools
import shutils

c = cdsapi.Client()

output_dir_root = os.path.join(archive_root, "data")
download_tmp_dir = os.path.join(output_dir_root, "tmp")
Path(download_tmp_dir).mkdir(parents=True, exist_ok=True)

def ifSkip(dt):

    skip = False

    return skip

nproc = 4


# There are 4 requests to completely download the data in 
# a given time. It is
#
#  1. UVTZ
#  2. Q
#  3. W
#  4. 19 surface layers (inst)
#  5. 9  surface layers (avg)
#  6. 9 2D ocean variables


def generateRequest(model_version_dt, start_dts, ens_type, varset):

    # Shared properties
    req = {
        "dataset": "s2s",
        "class": "s2",
        'format': 'netcdf',
        "expver": "prod",
        "model": "glob",
        "origin": "cwao",
        "stream": "enfh",
        "time": "00:00:00",
        "target": "output"
    }
     
    # Add in dates
    req["date"] = model_version_dt.strftime("%Y-%m-%d")
    req["hdate"] = [ start_dt.strftime("%Y-%m-%d") for start_dt in start_dts ].join("/")
    #"date": "2019-09-12",
    #"hdate": "1998-09-12/1999-09-12/2000-09-12/2001-09-12",
    
    # Add in ensemble information
    if ens_type == "ctl":
        
        req["type"] = "cf"

    elif ens_type == "pert"
        
        req["type"] = "pf"
        req["number"] = "1/2/3"

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
        
        # Surface inst (exclude: land-sea mask, orography, total precipitation(6hr), min/max 2m temp in the last 6hrs)
        req.update({
            "levtype": "sfc",
            "param": "134/146/147/151/165/166/169/175/176/177/179/174008/228143/228144",
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






beg_time = pd.Timestamp(year=beg_time.year, month=beg_time.month, day=1)
end_time = pd.Timestamp(year=end_time.year, month=end_time.month, day=1)

 


def doJob(req, output_file, output_dir):

    output_file_full = os.path.join(output_dir, output_file) 
    result = dict(output_file = output_file, output_file_full = output_file_full, req=req, status="UNKNOWN")

    tmp_file = os.path.join(download_tmp_dir, "%s.tmp" % output_file)
    req.update(dict(output=tmp_file))
    
    try:
        print("Downloading file: %s" % ( tmp_file, ))
        #c.retrieve(req)
        #shutils.move(tmp_file, output_file_full)

        result['status'] = 'OK'

    except Exception as e:

        result['status'] = 'ERROR'
        traceback.print_stack()
        traceback.print_exc()
        
        print(e)

    print("Finish generating file %s " % (output_file_full,))

    return result


failed_output_files = []

# Loop through a year
beg_year = 1998
end_year = 2017
years = np.arange(beg_year, end_year+1)
download_group_size_by_year = 5
dts_in_year = pd.date_range("2001-01-01", "2001-12-31", freq="D", inclusive="both")

year_groups = [
    years[download_group_size_by_year*i:download_group_size_by_year*(i+1)]
    for i in range(np.ceil(len(years) / download_group_size_by_year))
]

print("Year groups: ")
for i, year_group in enumerate(year_groups):
    print("[%d] %s" % (i, year_group.join(", ")) )


input_args = []
for model_version in ["GEPS5", "GEPS6"]:

    for dt in dts_in_year:
    
        model_version_date = tools.modelVersionReforecastDateToModelVersionDate(model_version, start_dt)

        if model_version_date is None:
            
            print("The date %s is not in the database. Skip." % (dt.strftime("%m/%d"))
            continue

        for year_group in year_groups:
            
            month = dt.month
            day = dt.day
            start_dts = pd.date_range(
                pd.Timestamp(year=year_group[0],  month=month, day=day),
                pd.Timestamp(year=year_group[-1], month=month, day=day),
                freq="Y",
                inclusive="both",
            )

            if len(start_dts) != len(year_group):
                raise Exception("Weird. Check.")
            
            
            for ens_type in ["ctl", "pert"]:

                for varset in ["UVTZ", "W", "Q", "surf_inst", "surf_avg", "ocn2d_avg"]:
                    

                    
                    output_dir = os.path.join(
                        output_dir_root,
                        model_version,
                        ens_type,
                        varset,
                    )
                    
                    Path(output_dir).mkdir(parents=True, exist_ok=True)
                    
                    output_file = "ECCC-S2S_{model_version:s}_{ens_type:s}_{varset:s}_{year_group_str:s}_{start_time:s}.nc".format(
                            model_version = model_version,
                            ens_type = ens_type,
                            varset = varset,
                            year_group_str = "%04d-%04d" % (year_group[0], year_group[-1]),
                            start_time     = "%02d-%02d" % (month, day),
                    )

                   
                    output_file_full = os.path.join(output_dir, output_file)

                    if os.path.exists(output_file_full):
                        
                        print("File %s already exists. Skip it." % (output_file_full,))
                        continue
                    
                    req = generateRequest(model_version_date, start_dts, ens_type, varset)
                    input_args.append((req, output_file, output_dir))
            

with Pool(processes=nproc) as pool:

    results = pool.starmap(doJob, input_args)

    for i, result in enumerate(results):
        if result['status'] != 'OK':
            print('!!! Failed to generate output file %s.' % (result['output_file_full'],))
            failed_dates.append(result['output_file_full'])


print("Tasks finished.")

print("Failed output files: ")
for i, failed_output_file in enumerate(failed_output_files):
    print("%d : %s" % (i+1, failed_output_file,))

print("Done.")
