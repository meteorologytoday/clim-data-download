from multiprocessing import Pool
import multiprocessing
import datetime
from pathlib import Path
import os.path
import os
import netCDF4

def pleaseRun(cmd):
    print(">> %s" % cmd)
    os.system(cmd)


def ifSkip(dt):

    skip = False

    """
    if dt.month in [5,6,7,8]:
        skip = True

    
    if dt.month == 4 and dt.day > 15:
        skip = True

    if dt.month == 9 and dt.day < 15:
        skip = True
    """

    return skip

# This is for AR
beg_time = datetime.datetime(1992,    9, 1)
end_time = datetime.datetime(2017,    5, 1)

# This is for ECCC s2s project
#beg_time = datetime.datetime(1997,    1,  1)
#end_time = datetime.datetime(2018,    2, 1)

# This is for ECMWFs2s, need to plus 45 days
#beg_time = datetime.datetime(2004,     1,  1)
#end_time = datetime.datetime(2023,     2,  28)

# This is for CW3E 2023 AR case
#beg_time = datetime.datetime(2023,     1,  1)
#end_time = datetime.datetime(2023,     2,  1)


archive_root = "data"


g0 = 9.81
