import xarray as xr
import numpy as np
from pathlib import Path


def genFilePath(dt, hours_skip, varname, root = "."):
    
    root = Path(root)
    
    return root / ("skiphrs-%d" % (hours_skip,)) / varname / "TRMM_{varname:s}_{dt:s}.nc".format(
        varname = varname,
        dt = dt.strftime("%Y-%m-%dT%H:%M"),
    )


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
 
