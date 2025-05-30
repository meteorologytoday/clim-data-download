import xarray as xr
import pandas as pd
import os
import PentadTools as ptt

data_archive = "data"

def _getFilename(dataset, datatype, varname, year: int, pentad: int, label=""):
    
    if datatype not in ["physical", "cropped", "spectral"]:
        raise Exception("Unknown datatype: %s" % (datatype,))

        
    if label == "":
        if datatype == "spectral":
            raise Exception("Spectral data needs parameter `label`.")

        elif datatype == "cropped":
            raise Exception("Cropped data needs parameter `label`.")


    full_filename = os.path.join(
        data_archive,
        datatype,
        label,
        varname,
        dataset,
        "{dataset:s}_{datatype:s}_{varname:s}_{year:04d}P{pentad:02d}.nc".format(
            dataset = dataset,
            datatype = datatype,
            varname = varname,
            year = year,
            pentad = pentad,
        ),
    )

    return full_filename 
 
def getFilenameFromTimePentad(dataset, datatype, varname, tp: ptt.TimePentad, label=""):
    return _getFilename(dataset, datatype, varname, tp.year, tp.pentad, label=label)
 
    
def load_dataset(dataset, datatype, varname, tp_beg, tp_end, label="", inclusive="left"):

    print("Check if parameters are fine...")
    data_interval = pd.Timedelta(days=1)

    """
    if dataset in ["oisst", "ostia", "MUR"]:
        
        data_interval = pd.Timedelta(days=1)

    else:

        raise Exception("Unknown dataset: %s" % (dataset,))
    """

    tp_beg = ptt.TimePentad(tp_beg)
    tp_end = ptt.TimePentad(tp_end)
 
    if tp_end < tp_beg:
        raise Exception("tp_end = %s should be later than tp_beg = %s" % ( tp_beg, tp_end, ))
    
     
    filenames = [
        getFilenameFromTimePentad(dataset, datatype, varname, tp, label=label)
        for tp in ptt.pentad_range(tp_beg, tp_end, inclusive=inclusive)
    ]
    
    sel_pentadstamps = [ tp.toPentadstamp() for tp in ptt.pentad_range(tp_beg, tp_end, inclusive=inclusive) ]
    print("sel_pentadstamps = ", sel_pentadstamps)
    
    print("Open dataset using xr.open_mfdataset...")
    ds = xr.open_mfdataset(filenames)
    
    print("Subsetting...")

    ds = ds.sel(pentadstamp = sel_pentadstamps)

    print("Done subsetting.")
    return ds
    
    

def __load_dataset(dataset, datatype, varname, year_beg, year_end, label=""):

    if dataset in ["ERA5", "oisst", ]:
        
        data_interval = pd.Timedelta(days=1)

    else:

        raise Exception("Unknown dataset: %s" % (dataset,))
        

    filenames = [
        getFilenameFromYear(dataset, datatype, varname, year, label=label)
        for year in range(year_beg, year_end+1)
    ]

    ds = xr.open_mfdataset(filenames)

    return ds
    
    









