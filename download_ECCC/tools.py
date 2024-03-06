import pandas as pd

# The bounds are included
model_version_date_bounds = dict(
    GEPS5 = [ pd.Timestamp("2018-09-27"), pd.Timestamp("2019-06-27"), ],
    GEPS6 = [ pd.Timestamp("2019-07-24"), pd.Timestamp("2021-11-25"), ],
)

valid_start_time_bnds = dict(
    GEPS5 = [ pd.Timestamp("1998-01-01"), pd.Timestamp("2017-12-31")],
    GEPS6 = [ pd.Timestamp("1998-01-01"), pd.Timestamp("2017-12-31")],
)


# Load the data
valid_model_version_dates = dict(GEPS5=[], GEPS6=[])
with open("model_version_dates.txt", "r") as f:
    for s in f.readlines():
        if s != "":
            ts = pd.Timestamp(s)
            for model_version in ["GEPS5", "GEPS6"]:
                bnds = model_version_date_bounds[model_version]
                if ts >= bnds[0] and ts <= bnds[1]:
                    valid_model_version_dates[model_version].append(ts)

            
for model_version in ["GEPS5", "GEPS6"]:
    valid_model_version_dates[model_version].sort(key=lambda ts: (ts.month, ts.day))
    
def printValidModelVersionDates():
     for model_version in ["GEPS5", "GEPS6"]:
        for i, model_version_date in enumerate(valid_model_version_dates[model_version]):
            print("[%s - %d] %s " % (model_version, i, model_version_date.strftime("%Y-%m-%d")))




"""
    This function receives model_version GEPS5 or GEPS6
    and reforecast date.

    The function returns model_version_date
"""
def modelVersionReforecastDateToModelVersionDate(model_version, reforecast_date):
    
    # IMPORTANT:
    # Notice that there is no two reforecast_date of the same model_version
    # map to two model_version_date
    # The model_version_date is unique with the same model_version
    # Therefore, a specific month-day will map to a specific model_version_date
    # for either GEPS5 or GEPS6

    _valid_model_version_dates = valid_model_version_dates[model_version]
    for i, valid_model_version_date in enumerate(_valid_model_version_dates):
        if valid_model_version_date.month == reforecast_date.month and \
           valid_model_version_date.day   == reforecast_date.day:
            
            return valid_model_version_date
            
        
    return None

if __name__ == "__main__":   

    printValidModelVersionDates()

    test_dates = dict(
        GEPS5 = ["2001-01-03", "2016-02-05", ],
        GEPS6 = ["2012-09-08", "1998-05-06", ],
    )

    for model_version, _test_dates in test_dates.items():
        for _test_date in _test_dates:
            _test_date_ts = pd.Timestamp(_test_date)
            print("[%s] Test the date %s maps to model version date " % (model_version, _test_date,), 
                modelVersionReforecastDateToModelVersionDate(model_version, _test_date_ts),
            )
    
    
    
