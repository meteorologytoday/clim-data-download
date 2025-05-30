import pandas as pd
import xarray as xr
from pathlib import Path

version = "stable_4kmD2"
archive_root = Path("data")
dataset_name_daily = "PRISM_%s" % (version,)

year_rng_clim = (1991, 2020)
dataset_name_clim = "PRISM_%s_clim_%d-%d" % (version, year_rng_clim[0], year_rng_clim[1])

dataset_dir_daily = archive_root / dataset_name_daily
dataset_dir_clim  = archive_root / dataset_name_clim

def getClimPRISMFilename(dt):

    PRISM_filename = dataset_dir_clim / "PRISM_clim-ppt-{md:s}.nc".format(
       md = dt.strftime("%m-%d"),
    )

    return PRISM_filename

def getPRISMFilename(dt):

    PRISM_filename = dataset_dir_daily / "PRISM_ppt_stable_4kmD2-{ymd:s}.nc".format(
       ymd = dt.strftime("%Y-%m-%d"),
    )

    return PRISM_filename


def loadDatasetWithTime(beg_dt, end_dt=None, inclusive="both"):

    if end_dt is None:
        end_dt = beg_dt

    dts = pd.date_range(beg_dt, end_dt, freq="D", inclusive=inclusive)

    filenames = [ getPRISMFilename(dt) for dt in dts ]

    ds = xr.open_mfdataset(filenames)

    return ds


def loadClimDatasetWithTime(beg_dt, end_dt=None, inclusive="both"):

    if end_dt is None:
        end_dt = beg_dt

    dts = pd.date_range(
        pd.Timestamp(year=2001, month=beg_dt.month, day=beg_dt.day),
        pd.Timestamp(year=2001, month=end_dt.month, day=end_dt.day),
        freq="D", inclusive=inclusive,
    )

    filenames = [ getClimPRISMFilename(dt) for dt in dts ]

    ds = xr.open_mfdataset(filenames)

    return ds

