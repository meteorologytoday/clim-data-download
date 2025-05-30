import pandas as pd
import xarray as xr
import os
import numpy as np
from pathlib import Path


def genFilePath(
    origin,
    model_version,
    nwp_type,
    varset,
    start_time,
    number,
    root = ".",
):

    root = Path(root)

    number_str = "%04d" % number

    return root / origin / model_version / nwp_type / varset / number_str / "{origin:s}_{model_version:s}_{nwp_type:s}_{varset:s}_{start_time:s}.{number:s}.nc".format(
        origin = origin,
        model_version = model_version,
        varset = varset,
        start_time  = start_time.strftime("%Y-%m-%d"),
        nwp_type = nwp_type,
        number = number_str,
    )


def open_dataset(
    origin,
    model_version,
    nwp_type,
    varset,
    start_time,
    numbers,
    root=".",
):


    filenames = []
    for number in numbers:
        
        loading_filename = genFilePath(
            origin,
            model_version,
            nwp_type,
            varset,
            start_time,
            number,
            root = root,
        )

        filenames.append(loading_filename)

    print(filenames)

    ds = xr.open_mfdataset(filenames)

    # Finally flip latitude
    lat = ds.coords["latitude"]
    if np.all( (lat[1:] - lat[:-1]) < 0 ):
        print("Flip latitude so that it is monotonically increasing")
        ds = ds.isel(latitude=slice(None, None, -1))

    return ds



if __name__ == "__main__":   

    # Part I: model dates and such
    print("Part I: Test model dates finding model version date")

    printValidModelVersionDates()

    test_dates = dict(
        GEPS5 = ["2001-01-03", "2016-02-05", ],
        GEPS6 = ["2012-09-08", "1998-05-06", ],
        GEPS6sub1 = ["2012-03-19", "1998-10-03", ],
    )

    for model_version, _test_dates in test_dates.items():
        for _test_date in _test_dates:
            _test_date_ts = pd.Timestamp(_test_date)
            print("[%s] Test the date %s maps to model version date " % (model_version, _test_date,), 
                modelVersionReforecastDateToModelVersionDate(model_version, _test_date_ts),
            )
    
    
    # Part II: Loading model data
    
    print("Part II: Test loading model data...")
    print("Current package's global variable `archive_root` = '%s'" % (archive_root,))
    varset = "Q"
    model_version = "GEPS5"
    start_time = pd.Timestamp("2017-01-03")
    step = slice(0, 5)

    ds = open_dataset(varset, model_version, start_time) 
    print(ds) 
 
